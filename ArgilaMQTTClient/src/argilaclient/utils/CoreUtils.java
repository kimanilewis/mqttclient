package argilaclient.utils;

import argilaclient.db.MySQL;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Utilities class.
 *
 * @author Brian Ngure
 */
@SuppressWarnings({"ClassWithoutLogger", "FinalClass"})
public final class CoreUtils {

    /**
     * Get the log prefix string.
     *
     * @return the log prefix string
     */
    public static String getLogPreString() {
        return "Checkout Status Poller | ";
    }

    /**
     * Formulate the status history.
     *
     * @param statHistory the status history
     * @param dte the date
     * @param oStatus the overall status
     * @param log log class instance
     *
     * @return the status history string
     */
    public static String formulateStatusHistory(final String statHistory,
            final String dte, final int oStatus, final long aID,
            final Logging log) {
        String newStatusHistory = "";
        try {
            JSONArray jsonArray;
            if ((statHistory == null) || (statHistory.isEmpty())
                    || (statHistory.equalsIgnoreCase(" "))
                    || !statHistory.startsWith("[")) {
                jsonArray = new JSONArray();
            } else {
                jsonArray = new JSONArray(statHistory);

            }

            JSONObject statusHistoryJSON = new JSONObject();
            statusHistoryJSON.put("date", dte);
            statusHistoryJSON.put("status", String.valueOf(oStatus));
            statusHistoryJSON.put("appID", String.valueOf(aID));

            jsonArray.put(statusHistoryJSON);

            newStatusHistory = jsonArray.toString();
        } catch (JSONException e) {
            log.error(getLogPreString() + "formulateStatusHistory() error: "
                    + e.getMessage());
        }

        return newStatusHistory;
    }

    /**
     * Execute the specified query to update a record.
     *
     * @param updateQuery the query
     * @param params the query parameters
     * @param mySQL mysql class instance
     * @param log log class instance
     * @param props properties instance
     *
     * @return the status of the update
     */
    public static int updateRecord(final String updateQuery,
            final List<Object> params, final MySQL mySQL, final Logging log,
            final Props props) {
        int result = 0;

        try (Connection conn = mySQL.getConnection();
                PreparedStatement stmt = conn.prepareStatement(updateQuery)) {
            int counter = 0;

            for (Object param : params) {
                ++counter;

                if (param instanceof Integer) {
                    stmt.setInt(counter, ((Integer) param));
                } else if (param instanceof String) {
                    stmt.setString(counter, (String) param);
                } else if (param instanceof Float) {
                    stmt.setFloat(counter, ((Float) param));
                } else if (param instanceof java.util.Date) {
                    stmt.setDate(counter, (java.sql.Date) param);
                } else if (param instanceof Boolean) {
                    stmt.setBoolean(counter, ((Boolean) param));
                } else if (param instanceof Long) {
                    stmt.setLong(counter, ((Long) param));
                } else {
                    stmt.setString(counter, (String) param);
                }
            }

            result = stmt.executeUpdate();

            if (result > 0) {
                log.info(getLogPreString() + "Update record was successfull");
            }
        } catch (SQLException e) {
            log.error(getLogPreString() + "Update record error: "
                    + e.getMessage());
            log.info(getLogPreString() + "Invoking failsafe => updateFile()");

            String query = prepareRowQueryFromPreparedPayload(updateQuery,
                    params);
            updateFailedQueriesFile(props.getFailedQueryFile(), query, log);
        }

        return result;
    }

    /**
     * Prepare the update query (clean it).
     *
     * @param updateQuery the query
     * @param params the parameters
     *
     * @return the prepared query
     */
    public static String prepareRowQueryFromPreparedPayload(
            final String updateQuery, final List<Object> params) {
        String query = updateQuery.replaceAll("[?]", "'%s'");
        query = String.format(query.replace("\\n", ""), params.toArray());

        return query;
    }

    /**
     * Write to the failed queries file.
     *
     * @param file the file name
     * @param data the data to write
     * @param log log class instance
     */
    public static void updateFailedQueriesFile(final String file,
            final String data, final Logging log) {
        log.info(CoreUtils.getLogPreString() + "Writing failed query: [" + data
                + "] to file: " + file);

        File queryFile = new File(file);

        try (PrintWriter pout = new PrintWriter(queryFile)) {
            queryFile.createNewFile();

            pout.println(data);
            pout.close();

            log.info(CoreUtils.getLogPreString() + "Written failed query: ["
                    + data + "] to file: " + file);
        } catch (IOException | SecurityException ex) {
            log.error(CoreUtils.getLogPreString() + "Error writing failed "
                    + "query: [" + data + "] to file: " + file + ". Reason: "
                    + ex.getMessage());
        }
    }

    /**
     * Remove an updated query from the failed query file.
     *
     * @param queryfile Failed query file path
     * @param query Query to be removed from file
     * @param log Logging instance
     */
    public static void deleteQuery(final String queryfile, final String query,
            final Logging log) {
        ArrayList<String> queries = new ArrayList<>(0);
        try (FileInputStream fin = new FileInputStream(queryfile);
                DataInputStream in = new DataInputStream(fin);
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(in))) {
            String data;
            while ((data = br.readLine()) != null) {
                if (!queries.contains(data)) {
                    queries.add(data);
                }
            }

            /*--Find a match to the query--*/
            log.error(CoreUtils.getLogPreString()
                    + "About to remove this query: " + query + " from file: "
                    + queryfile);
            if (queries.contains(query)) {
                queries.remove(query);
                log.info(CoreUtils.getLogPreString()
                        + "I have removed this query: " + query + " from file: "
                        + queryfile);
            }

            // Now save the file
            try (PrintWriter pout = new PrintWriter(
                    new FileOutputStream(queryfile, false))) {
                for (String remainingQuery : queries) {
                    pout.println(remainingQuery);
                }
            }
        } catch (Exception e) {
            log.error(CoreUtils.getLogPreString() + "Error while deleting query"
                    + " from file. Exception message : " + e.getMessage());
        }
    }

    /**
     * Prepares the statement to store in the file.
     *
     * @param query the query string
     * @param params the parameters
     * @param index the parameter index
     *
     * @return the prepared query string
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public static String prepareSqlString(final String query,
            final String[] params, final int index) {
        int localIndex = index;

        if (!query.contains("?")) {
            return query;
        }

        String s = query.replaceFirst("\\?", params[localIndex]);

        return prepareSqlString(s, params, ++localIndex);
    }

    /**
     * Check if a given state exists in array. Cannot use Arrays class as does
     * not work with primitive arrays.
     *
     * @param currentState the item to check
     * @param myArray the array to check
     *
     * @return true if exists, false otherwise
     */
    public static boolean checkInArray(final int currentState,
            final int[] myArray) {
        for (int i : myArray) {
            if (i == currentState) {
                return true;
            }
        }

        return false;
    }

    /**
     * Append data to a file.
     *
     * @param logging the logger
     * @param filepath the file
     * @param data the string to write
     *
     * @return true if written successfully, false otherwise
     */
    @SuppressWarnings({"null", "ConstantConditions"})
    private static Boolean writeToFile(final Logging logging,
            final String filepath, final String data) {
        String logPreString = CoreUtils.getLogPreString() + " | writeToFile() | ";

        try (PrintWriter pout = new PrintWriter(
                new FileOutputStream(filepath, true))) {
            pout.println(data);
            pout.flush();

            logging.info(logPreString
                    + "Appended query: " + data + " to file: " + filepath);
            return true;
        } catch (IOException ex) {
            logging.fatal(logPreString + "Failed to append query: "
                    + data + " to file: "
                    + filepath + ". Error: " + ex.getMessage());

            return false;
        }
    }

    /**
     * <p>
     * Store failed post profile ACKs in a file. NOTE: Checks whether the file
     * exists and writes to it the ACKs that need to be performed.</p>
     *
     * <p>
     * Performance Issues: This has an effect on the speed of execution. Speed
     * reduces because of waiting for a lock to be unlocked. The wait time is
     * unlimited.</p>
     *
     * @param logging the logger
     * @param file the file to write to
     * @param data the string to write
     *
     * @return true if updated, false otherwise
     */
    @SuppressWarnings({"null", "ConstantConditions"})
    public static boolean updateFile(final Logging logging, final String file,
            final String data) {
        String logPreString = CoreUtils.getLogPreString() + "| updateFile() | ";

        boolean status = false;
        File myfile = new File(file);
        if (!myfile.exists()) {
            logging.info(logPreString
                    + " Post profile ACKs or debits file was not found, "
                    + "creating and appending file");

            try {
                if (myfile.createNewFile()) {
                    logging.info(logPreString + " Failed post profile ACK or "
                            + "debit: " + data + ", appending to file");
                    status = writeToFile(logging, file, data);
                }
            } catch (IOException ex) {
                logging.fatal(logPreString + "Unable to create file. Error: "
                        + ex.getMessage());

                status = false;
            }
        } else {
            logging.info(logPreString
                    + " Failed post profile ACK or debit: " + data
                    + ", appending to file");
            status = writeToFile(logging, file, data);
        }

        return status;
    }

    /**
     * Private constructor.
     */
    private CoreUtils() {
        super();
    }
}
