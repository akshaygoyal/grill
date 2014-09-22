package com.inmobi.grill.cli.commands;

/*
 * #%L
 * Grill CLI
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Joiner;
import com.inmobi.grill.api.query.*;
import com.inmobi.grill.client.GrillClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

@Component
public class GrillQueryCommands extends  BaseGrillCommand implements CommandMarker {

  @CliCommand(value = "query execute", help = "Execute query in async/sync manner")
  public String executeQuery(
      @CliOption(key = {"", "query"}, mandatory = true, help = "Query to execute") String sql,
      @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch,
      @CliOption(key = {"store"}, mandatory = false, help = "Location to store result file") String location) {

    // This is to check if storage location is provided or not,
    // this will also handle this case: 'query execute <query> --store '
    // in which it ignores the --store option and proceed.
    if (isLocationProvided(location)) {
      // This is to check if provided path is valid dir or not.
      LOG.debug("***** Location provided: " + location);
      if(!isLocationValid(location)){
        LOG.debug("***** Location invalid: " + location);
        return "Provided storage location either does not exist or is not a directory, please check the path.";
      }
      LOG.debug("***** Location valid: " + location);
    }

    if (!asynch) {
      try {
        GrillClient.GrillClientResultSetWithStats result = getClient().getResults(sql);
        return formatResultSet(result, location);
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executeQueryAsynch(sql);
      return handle.getHandleId().toString();
    }
  }


  private boolean isLocationProvided(String location) {
    if (location != null && !location.isEmpty()) {
      return true;
    }
    return false;
  }

  private boolean isLocationValid(String location) {
    File file = new File(location);
    if (file.exists() && file.isDirectory()) {
      return true;
    }
    return false;
  }


  private String formatResultSet(GrillClient.GrillClientResultSetWithStats rs,
      String location) {
    if (rs.getResultSet() != null) {
      QueryResultSetMetadata resultSetMetadata = rs.getResultSet().getResultSetMetadata();
      QueryResult r = rs.getResultSet().getResult();

      if (r instanceof InMemoryQueryResult) {
        return getInMemoryResults(rs, r, resultSetMetadata, location);
      } else {
        return getPersistentQueryResults(rs, r, location);
      }
    }
    return getRowCountMessage(rs, 0);
  }

  private String getInMemoryResults(
      GrillClient.GrillClientResultSetWithStats rs,
      QueryResult r, QueryResultSetMetadata resultSetMetadata, String location) {
    LOG.debug("***** In getInMemmoryResults ");
    StringBuilder str = new StringBuilder();
    int rowCount = 0;
    for (ResultColumn column : resultSetMetadata.getColumns()) {
      str.append(column.getName()).append("\t");
    }
    str.append("\n");
    InMemoryQueryResult temp = (InMemoryQueryResult) r;
    for (ResultRow row : temp.getRows()) {
      for (Object col : row.getValues()) {
        str.append(col).append("\t");
      }
      rowCount++;
      str.append("\n");
    }

    str.append(getRowCountMessage(rs, rowCount));

    if (isLocationProvided(location)) {
      String dirName = location + "/" + rs.getQuery().getQueryHandle().getHandleId();
      File dir = new File(dirName);
      File file;
      try {
        dir.mkdir();
        String fileName = dirName + "/result.txt";
        file = new File(fileName);
        if (!file.exists()) {
          file.createNewFile();
        }

        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(str.toString());
        fileWriter.close();
      } catch (IOException ioException) {
        return ioException.getMessage();
      }
      return "Results stored at " + file.getAbsolutePath();
    }
    return str.toString();
  }

  private String getPersistentQueryResults(GrillClient.GrillClientResultSetWithStats rs, QueryResult r, String location) {
    if (isLocationProvided(location)) {
      LOG.debug("***** In getPersistentQueryResults ");
      LOG.debug("***** In getPersistentQueryResults: Query Handle: " + rs.getQuery().getQueryHandle().toString());
      Response response = getClient().getHTTPResultSet(
          rs.getQuery().getQueryHandle());
      //      Response response = client.getHTTPResultSet(QueryHandle.fromString("6108328f-3039-47b9-b87c-b494f92d9fef"));
      LOG.debug(response.toString());
      return "Response: " + response.getEntity();
      //      ReadableByteChannel rbc = Channels.newChannel((new URL("").openStream());
      //      FileOutputStream fos = null;
      //      try {
      //        fos = new FileOutputStream(location);
      //        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
      //      } catch (IOException e) {
      //        e.printStackTrace();
      //      }
      //    } else {
      //      StringBuilder str = new StringBuilder();
      //      PersistentQueryResult temp = (PersistentQueryResult) r;
      //      str.append("Results of query stored at : " + temp.getPersistedURI());
      //    }
    }
    return null;
  }

  private String getRowCountMessage(
      GrillClient.GrillClientResultSetWithStats rs, int rowCount) {
    StringBuilder message = new StringBuilder();
    if (rs.getQuery() != null) {
      long submissionTime = rs.getQuery().getSubmissionTime();
      long endTime = rs.getQuery().getFinishTime();
      message.append(rowCount).append(" rows process in (").
          append(endTime > 0 ? ((endTime - submissionTime) / 1000) : 0).
          append(") seconds.\n");
    }
    return message.toString();
  }


  @CliCommand(value = "query status", help = "Fetch status of executed query")
  public String getStatus(@CliOption(key = {"", "query"},
      mandatory = true, help = "<query-handle> for which status has to be fetched") String qh) {
    QueryStatus status = getClient().getQueryStatus(new QueryHandle(UUID.fromString(qh)));
    StringBuilder sb = new StringBuilder();
    if(status == null) {
      return "Unable to find status for " + qh;
    }
    sb.append("Status : ").append(status.getStatus()).append("\n");
    if (status.getStatusMessage() != null) {
      sb.append("Message : ").append(status.getStatusMessage()).append("\n");
    }
    if (status.getProgress() != 0) {
      sb.append("Progress : ").append(status.getProgress()).append("\n");
      if (status.getProgressMessage() != null) {
        sb.append("Progress Message : ").append(status.getProgressMessage()).append("\n");
      }
    }

    if (status.getErrorMessage() != null) {
      sb.append("Error : ").append(status.getErrorMessage()).append("\n");
    }

    return sb.toString();
  }

  @CliCommand(value = "query explain", help = "Explain query plan")
  public String explainQuery(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to execute") String sql, @CliOption(key = {"save"},
      mandatory = false, help = "query to explain") String location)
      throws UnsupportedEncodingException {
    
    QueryPlan plan = getClient().getQueryPlan(sql);
    if (plan.isHasError() == true) {
      return plan.getErrorMsg();
    }
    return plan.getPlanString();
  }

  @CliCommand(value = "query list", help = "Get all queries")
  public String getAllQueries(@CliOption(key = {"state"}, mandatory = false,
      help = "Status of queries to be listed") String state, @CliOption(key = {"user"}, mandatory = false,
      help = "User of queries to be listed") String user) {
    List<QueryHandle> handles = getClient().getQueries(state, user);
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No queries";
    }
  }

  @CliCommand(value = "query kill", help ="Kill a query")
  public String killQuery(@CliOption(key = {"", "query"},
      mandatory = true, help = "query-handle for killing") String qh) {
    boolean status = getClient().killQuery(new QueryHandle(UUID.fromString(qh)));
    if(status) {
      return "Successfully killed " + qh;
    } else {
      return "Failed in killing "  + qh;
    }
  }

  @CliCommand(value = "query results", help ="get results of async query")
  public String getQueryResults(@CliOption(key = {"", "query"},
      mandatory = true, help = "query-handle for fetching result") String qh,
      @CliOption(key = {"store"}, mandatory = false, help = "Location to store result file") String location)   {

    // This is to check if storage location is provided or not,
    // this will also handle this case: 'query results <query-handle> --store '
    // in which it ignores the --store option and proceed.
    if (location != null && !location.isEmpty()) {
      // This is to check if provided path is valid dir or not.
      if(!isLocationValid(location)){
        return "Provided storage location either does not exist or is not a directory, please check the path.";
      }
    }

    try {
      GrillClient.GrillClientResultSetWithStats result = getClient().getAsyncResults(
          new QueryHandle(UUID.fromString(qh)));
      if(location != null && !location.isEmpty()){
        return formatResultSet(result, location);
      } else {
        return formatResultSet(result, location);
      }
    } catch (Throwable t) {
      return t.getMessage();
    }
  }

  @CliCommand(value = "prepQuery list", help = "Get all prepared queries")
  public String getAllPreparedQueries() {
    List<QueryPrepareHandle> handles = getClient().getPreparedQueries();
    if (handles != null && !handles.isEmpty()) {
      return Joiner.on("\n").skipNulls().join(handles);
    } else {
      return "No prepared queries";
    }
  }

  @CliCommand(value = "prepQuery details", help = "Get prepared query")
  public String getPreparedStatus(@CliOption(key = {"", "handle"},
      mandatory = true, help = "Prepare handle") String ph) {
    GrillPreparedQuery prepared = getClient().getPreparedQuery(QueryPrepareHandle.fromString(ph));
    if (prepared != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("User query:").append(prepared.getUserQuery()).append("\n");
      sb.append("Prepare handle:").append(prepared.getPrepareHandle()).append("\n");
      sb.append("User:" + prepared.getPreparedUser()).append("\n");
      sb.append("Prepared at:").append(prepared.getPreparedTime()).append("\n");
      sb.append("Selected driver :").append(prepared.getSelectedDriverClassName()).append("\n");
      sb.append("Driver query:").append(prepared.getDriverQuery()).append("\n");
      if (prepared.getConf() != null) {
        sb.append("Conf:").append(prepared.getConf().getProperties()).append("\n");
      }

      return sb.toString();
    } else {
      return "No such handle";
    }
  }

  @CliCommand(value = "prepQuery destroy", help ="Destroy a prepared query")
  public String destroyPreparedQuery(@CliOption(key = {"", "handle"},
      mandatory = true, help = "prepare handle to destroy") String ph) {
    boolean status = getClient().destroyPrepared(new QueryPrepareHandle(UUID.fromString(ph)));
    if(status) {
      return "Successfully destroyed " + ph;
    } else {
      return "Failed in destroying "  + ph;
    }
  }

  @CliCommand(value = "prepQuery execute", help = "Execute prepared query in async/sync manner")
  public String executePreparedQuery(
      @CliOption(key = {"", "handle"}, mandatory = true, help = "Prepare handle to execute") String phandle,
      @CliOption(key = {"async"}, mandatory = false, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true", help = "Sync query execution") boolean asynch,
      @CliOption(key = {"store"}, mandatory = false, help = "Location to store result file") String location) {

    // This is to check if storage location is provided or not,
    // this will also handle this case: 'prepQuery execute <query-handle> --store '
    // in which it ignores the --store option and proceed.
    if (location != null && !location.isEmpty()) {
      // This is to check if provided path is valid dir or not.
      if(!isLocationValid(location)){
        return "Provided storage location either does not exist or is not a directory, please check the path.";
      }
    }

    if (!asynch) {
      try {
        GrillClient.GrillClientResultSetWithStats result =
            getClient().getResultsFromPrepared(
                QueryPrepareHandle.fromString(phandle));
        if(location != null && !location.isEmpty()){
          return formatResultSet(result, location);
        } else {
          return formatResultSet(result, location);
        }
      } catch (Throwable t) {
        return t.getMessage();
      }
    } else {
      QueryHandle handle = getClient().executePrepared(QueryPrepareHandle.fromString(phandle));
      return handle.getHandleId().toString();
    }
  }

  @CliCommand(value = "prepQuery prepare", help = "Prepapre query")
  public String prepare(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to prepare") String sql)
      throws UnsupportedEncodingException {

    QueryPrepareHandle handle = getClient().prepare(sql);
    return handle.toString();
  }

  @CliCommand(value = "prepQuery explain", help = "Explain and prepare query")
  public String explainAndPrepare(@CliOption(key = {"", "query"}, mandatory = true,
      help = "Query to explain and prepare") String sql)
      throws UnsupportedEncodingException {

    QueryPlan plan = getClient().explainAndPrepare(sql);
    StringBuilder planStr = new StringBuilder(plan.getPlanString());
    planStr.append("\n").append("Prepare handle:").append(plan.getPrepareHandle());
    return planStr.toString();
  }

}
