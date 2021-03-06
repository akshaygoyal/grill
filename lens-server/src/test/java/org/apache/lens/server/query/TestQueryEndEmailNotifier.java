/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query;

import static org.apache.lens.server.common.RestAPITestUtil.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.InMemoryQueryResult;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.LensTestUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.common.TestResourceFile;

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;
import org.testng.annotations.*;

import com.google.common.base.Optional;

/**
 * The query completion email notifier
 */
@Test(groups = "unit-test")
public class TestQueryEndEmailNotifier extends LensJerseyTest {

  private static final int NUM_ITERS = 30;
  /** The query service. */
  QueryExecutionServiceImpl queryService;

  /** The lens session id. */
  LensSessionHandle lensSessionId;

  /** The wiser. */
  private Wiser wiser;

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#setUp()
   */
  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
    wiser = new Wiser();
    wiser.setHostname("localhost");
    wiser.setPort(25000);
    queryService = LensServices.get().getService(QueryExecutionService.NAME);
    Map<String, String> sessionconf = new HashMap<>();
    sessionconf.put("test.session.key", "svalue");
    sessionconf.put(LensConfConstants.QUERY_MAIL_NOTIFY, "true");
    sessionconf.put(LensConfConstants.QUERY_RESULT_EMAIL_CC, "foo1@localhost,foo2@localhost,foo3@localhost");
    lensSessionId = queryService.openSession("foo@localhost", "bar", sessionconf); // @localhost should be removed
    // automatically
    createTable(TEST_TABLE);
    loadData(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue());
    wiser.start();
  }

  @BeforeMethod
  public void clearWiserMessages() {
    wiser.getMessages().clear();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    wiser.stop();
    dropTable(TEST_TABLE);
    queryService.closeSession(lensSessionId);
    super.tearDown();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configure()
   */
  @Override
  protected Application configure() {
    return new QueryApp();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#configureClient(org.glassfish.jersey.client.ClientConfig)
   */
  @Override
  protected void configureClient(ClientConfig config) {
    config.register(MultiPartFeature.class);
  }

  /** The test table. */
  public static final String TEST_TABLE = "EMAIL_NOTIFIER_TEST_TABLE";

  /**
   * Creates the table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void createTable(String tblName) throws InterruptedException {
    LensTestUtil.createTable(tblName, target(), lensSessionId);
  }

  /**
   * Load data.
   *
   * @param tblName      the tbl name
   * @param testDataFile the test data file
   * @throws InterruptedException the interrupted exception
   */
  private void loadData(String tblName, final String testDataFile) throws InterruptedException {
    LensTestUtil.loadDataFromClasspath(tblName, testDataFile, target(), lensSessionId);
  }

  /**
   * Drop table.
   *
   * @param tblName the tbl name
   * @throws InterruptedException the interrupted exception
   */
  private void dropTable(String tblName) throws InterruptedException {
    LensTestUtil.dropTable(tblName, target(), lensSessionId);
  }

  private QueryHandle launchAndWaitForQuery(LensConf conf, String query, Status expectedStatus)
    throws InterruptedException {
    return executeAndWaitForQueryToFinish(target(), lensSessionId, query, Optional.of(conf),
      Optional.of(expectedStatus)).getQueryHandle();
  }

  private WiserMessage getMessage() throws InterruptedException {
    List<WiserMessage> messages = new ArrayList<>();
    for (int i = 0; i < NUM_ITERS; i++) {
      messages = wiser.getMessages();
      if (messages.size() >= 4) {
        break;
      }
      Thread.sleep(2000);
    }
    assertEquals(messages.size(), 4);
    return messages.get(0);
  }

  private void assertKeywordsContains(Object o, Object... keywords) {
    String string = o.toString();
    for (Object keyword : keywords) {
      // Mail message converts \n to \r\n
      assertTrue(string.contains(keyword.toString().replaceAll("\\n", "\r\n")), o + " doesn't contain " + keyword);
    }
  }

  /**
   * Test launch fail.
   *
   * @throws InterruptedException the interrupted exception
   */
  @Test
  public void testLaunchFailure() throws InterruptedException {
    // launch failure
    final Response response = execute(target(), Optional.of(lensSessionId), Optional.of("select fail from non_exist"));
    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    QueryHandle handle = response.readEntity(new GenericType<LensAPIResult<QueryHandle>>() {}).getData();
    assertKeywordsContains(getMessage(), handle, "Launching query failed", "Reason");
  }

  @Test
  public void testFormattingFailure() throws InterruptedException {
    // formatting failure
    LensConf conf = getLensConf(
      LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true",
      LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false",
      LensConfConstants.QUERY_OUTPUT_SERDE, "NonexistentSerde.class");
    QueryHandle handle = launchAndWaitForQuery(conf, "select ID, IDSTR from " + TEST_TABLE, Status.FAILED);
    assertKeywordsContains(getMessage(), handle, "Result formatting failed!", "Reason");
  }

  @Test
  public void testExecutionFailure() throws InterruptedException {
    // execution failure
    LensConf conf = getLensConf(
      LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true",
      HiveConf.ConfVars.COMPRESSRESULT.name(), "true",
      "mapred.compress.map.output", "true",
      "mapred.map.output.compression.codec", "nonexisting");
    QueryHandle handle = launchAndWaitForQuery(conf, "select count(ID) from " + TEST_TABLE, Status.FAILED);
    assertKeywordsContains(getMessage(), handle, "Query execution failed!", "Reason");
  }

  @DataProvider(name = "success-tests")
  public Object[][] persistenceConfigDataProvider() {
    return new Object[][]{
      {false, false, },
      {true, false, },
      {false, true, },
      {true, true, },
    };
  }

  @Test(dataProvider = "success-tests")
  public void testSuccessfulQuery(Boolean lensPersistence, Boolean driverPersistence) throws InterruptedException {
    // successful query
    LensConf conf = getLensConf(
      LensConfConstants.QUERY_PERSISTENT_RESULT_SET, lensPersistence,
      LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, driverPersistence);
    QueryHandle handle = launchAndWaitForQuery(conf, "select ID, IDSTR from " + TEST_TABLE, Status.SUCCESSFUL);
    String expectedKeywords;
    if (lensPersistence || driverPersistence) {
      QueryResult result = getLensQueryResult(target(), lensSessionId, handle);
      expectedKeywords = result.toPrettyString();
    } else {
      expectedKeywords = InMemoryQueryResult.DECLARATION;
    }
    WiserMessage message = getMessage();
    assertKeywordsContains(message, handle, "Query SUCCESSFUL", expectedKeywords);
    if (lensPersistence) {
      assertKeywordsContains(message, new String[]{"Downloadable from", "httpresultset"});
      assertEquals(getLensQueryHttpResult(target(), lensSessionId, handle).getStatus(),
        Response.Status.OK.getStatusCode());
    } else {
      assertEquals(getLensQueryHttpResult(target(), lensSessionId, handle).getStatus(),
        Response.Status.NOT_FOUND.getStatusCode());
    }
  }
}
