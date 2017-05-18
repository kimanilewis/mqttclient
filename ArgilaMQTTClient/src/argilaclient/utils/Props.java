package argilaclient.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Loads system properties from a file.
 *
 * @author <a href="kimanilewi@gmail.com">Lewis Kimani</a>
 */
@SuppressWarnings({"FinalClass", "ClassWithoutLogger"})
public final class Props {

    /**
     * A list of any errors that occurred while loading the properties.
     */
    private final List<String> loadErrors;
    /**
     * Info log level. Default = INFO.
     */
    private String infoLogLevel = "INFO";
    /**
     * Error log level. Default = FATAL.
     */
    private String errorLogLevel = "ERROR";
    /**
     * failed Queries path
     */
    private String failedQueries;
    /**
     * Database connection pool name.
     */
    private String dbPoolName;
    /**
     * Database user name.
     */
    private String dbUserName;
    /**
     * Database password.
     */
    private String dbPassword;
    /**
     * Database host.
     */
    private String dbHost;
    /**
     * Database port.
     */
    private String dbPort;
    /**
     * Database name.
     */
    private String dbName;
    /**
     * Database max connections.
     */
    private int dbMaxConnections;
    /**
     * Query limit.
     */
    private int queryLimit;
    /**
     * Info log file name.
     */
    private String infoLogFile;
    /**
     * Error log file name.
     */
    private String errorLogFile;
    /**
     * No of threads that will be created in the thread pool to process
     * profiles.
     */
    private int numOfChildren;

    private int processing;
    /**
     * Processed State.
     */
    private int processed;
    /**
     * Processing Failed state.
     */
    private int processingFinished;
    /**
     * Maximum time a customer can have.
     */

    /**
     * Not Precessed.
     */
    private int unProcessed;
    /**
     * Cost Of Time Per Minute.
     */
    private int costOfTimePerMinute;
    /**
     * Authentication success status code.
     */
    private int authSuccessCode;

    /**
     * API connection timeout.
     */
    private int connectionTimeout;
    /**
     * API reply timeout.
     */
    private int replyTimeout;
    /**
     * Sleep time.
     */
    private int sleepTime;
    /**
     * The properties file.
     */
    private static final String PROPS_FILE = "conf/ArgilaClientProperties.xml";
    private String topic;
    private String accountsTopic;
    private String clientID;
    private String ackTopic;
    private int QoS;
    private String brokerUrl;

    /**
     * Constructor.
     */
    public Props() {
        loadErrors = new ArrayList<>(0);
        loadProperties(PROPS_FILE);
    }

    /**
     * Load system properties.
     *
     * @param propsFile the system properties XML file
     */
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private void loadProperties(final String propsFile) {
        Properties props = new Properties();

        try (FileInputStream propsStream = new FileInputStream(propsFile)) {
            props.loadFromXML(propsStream);

            String error1 = "ERROR: %s is <= 0 or may not have been set";
            String error2 = "ERROR: %s may not have been set";

            infoLogLevel = props.getProperty("InfoLogLevel");
            if (getInfoLogLevel().isEmpty()) {
                getLoadErrors().add(String.format(error2, "InfoLogLevel"));
            }

            errorLogLevel = props.getProperty("ErrorLogLevel");
            if (getErrorLogLevel().isEmpty()) {
                getLoadErrors().add(String.format(error2, "ErrorLogLevel"));
            }

            infoLogFile = props.getProperty("InfoLogFile");
            if (getInfoLogFile().isEmpty()) {
                getLoadErrors().add(String.format(error2, "InfoLogFile"));
            }

            errorLogFile = props.getProperty("ErrorLogFile");
            if (getErrorLogFile().isEmpty()) {
                getLoadErrors().add(String.format(error2, "ErrorLogFile"));
            }

            failedQueries = props.getProperty("FAILED_QUERY_FILE");
            if (getFailedQueryFile().isEmpty()) {
                getLoadErrors().add(String.format(error2, "FAILED_QUERY_FILE"));
            }

            String noc = props.getProperty("NumberOfThreads");
            if (noc.isEmpty()) {
                getLoadErrors().add(String.format(error1, "NumberOfThreads"));
            } else {
                numOfChildren = Integer.parseInt(noc);
                if (numOfChildren <= 0) {
                    getLoadErrors().add(String.format(error1,
                            "NumberOfThreads"));
                }
            }
            topic = props.getProperty("Topic");
            if (getTopic().isEmpty()) {
                getLoadErrors().add(String.format(error2, "Topic"));
            }
            accountsTopic = props.getProperty("AccountsTopic");
            if (getAccountsTopic().isEmpty()) {
                getLoadErrors().add(String.format(error2, "AccountsTopic"));
            }
            ackTopic = props.getProperty("AckTopic");
            if (getAckTopic().isEmpty()) {
                getLoadErrors().add(String.format(error2, "AckTopic"));
            }
            clientID = props.getProperty("ClientID");
            if (getClientID().isEmpty()) {
                getLoadErrors().add(String.format(error2, "ClientID"));
            }
            brokerUrl = props.getProperty("BrokerURL");
            if (getBrokerUrl().isEmpty()) {
                getLoadErrors().add(String.format(error2, "BrokerURL"));
            }

            String qos = props.getProperty("QoS");
            if (qos.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "QoS"));
            } else {
                QoS = Integer.parseInt(qos);
                if (QoS < 0) {
                    getLoadErrors().add(String.format(error1,
                            "QoS"));
                }
            }

            String connTimeout = props.getProperty("ConnectionTimeout");
            if (connTimeout.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "ConnectionTimeout"));
            } else {
                connectionTimeout = Integer.parseInt(connTimeout);
                if (connectionTimeout < 0) {
                    getLoadErrors().add(String.format(error1,
                            "ConnectionTimeout"));
                }
            }

            String replyTO = props.getProperty("ReplyTimeout");
            if (replyTO.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "ReplyTimeout"));
            } else {
                replyTimeout = Integer.parseInt(replyTO);
                if (replyTimeout < 0) {
                    getLoadErrors().add(String.format(error1,
                            "ReplyTimeout"));
                }
            }

            String limit = props.getProperty("QueryLimit");
            if (limit.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "QueryLimit"));
            } else {
                queryLimit = Integer.parseInt(limit);
                if (queryLimit < 0) {
                    getLoadErrors().add(String.format(error1,
                            "QueryLimit"));
                }
            }

            String processingStatus = props.getProperty("Processing");
            if (processingStatus.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "Processing"));
            } else {
                processing = Integer.parseInt(processingStatus);
                if (processing < 0) {
                    getLoadErrors().add(String.format(error1,
                            "Processing"));
                }
            }
            String processedStatus = props.getProperty("Processed");
            if (processedStatus.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "Processed"));
            } else {
                processed = Integer.parseInt(processedStatus);
                if (processed < 0) {
                    getLoadErrors().add(String.format(error1,
                            "Processed"));
                }
            }
            String unProcessedStatus = props.getProperty("UnProcessed");
            if (unProcessedStatus.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "UnProcessed"));
            } else {
                unProcessed = Integer.parseInt(unProcessedStatus);
                if (unProcessed < 0) {
                    getLoadErrors().add(String.format(error1,
                            "UnProcessed"));
                }
            }
            String finishedProcessingStatus = props.getProperty("ProcessingFinished");
            if (finishedProcessingStatus.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "ProcessingFinished"));
            } else {
                processingFinished = Integer.parseInt(finishedProcessingStatus);
                if (processingFinished < 0) {
                    getLoadErrors().add(String.format(error1,
                            "ProcessingFinished"));
                }
            }

            String sleep = props.getProperty("SleepTime");
            if (sleep.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "SleepTime"));
            } else {
                sleepTime = Integer.parseInt(sleep);
                if (sleepTime < 0) {
                    getLoadErrors().add(String.format(error1,
                            "SleepTime"));
                }
            }

            String as = props.getProperty("AuthenticationSuccess");
            if (as.isEmpty()) {
                getLoadErrors().add(String.format(error1,
                        "AuthenticationSuccess"));
            } else {
                authSuccessCode = Integer.parseInt(as);
                if (authSuccessCode < 0) {
                    getLoadErrors().add(String.format(error1,
                            "AuthenticationSuccess"));
                }
            }

            dbPoolName = props.getProperty("DbPoolName");
            if (getDbPoolName().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbPoolName"));
            }

            dbUserName = props.getProperty("DbUserName");
            if (getDbUserName().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbUserName"));
            }

            dbPassword = props.getProperty("DbPassword");
            if (getDbPassword().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbPassword"));
            }

            dbHost = props.getProperty("DbHost");
            if (getDbHost().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbHost"));
            }

            dbPort = props.getProperty("DbPort");
            if (getDbPort().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbPort"));
            }

            dbName = props.getProperty("DbName");
            if (getDbName().isEmpty()) {
                getLoadErrors().add(String.format(error2, "DbName"));
            }

            String maxConns = props.getProperty("DbMaxConnections");
            if (!maxConns.isEmpty()) {
                dbMaxConnections = Integer.parseInt(maxConns);
                if (dbMaxConnections <= 0) {
                    getLoadErrors().add(String.format(error1,
                            "DbMaxConnections"));
                }
            } else {
                getLoadErrors().add(String.format(error1, "DbMaxConnections"));
            }
        } catch (NumberFormatException ne) {
            System.err.println("Exiting. String value found, Integer is "
                    + "required: " + ne.getMessage());

            System.exit(1);
        } catch (FileNotFoundException ne) {
            System.err.println("Exiting. Could not find the properties file: "
                    + ne.getMessage());

            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Exiting. Failed to load system properties: "
                    + ioe.getMessage());

            System.exit(1);
        }
    }

    /**
     * Info log level. Default = INFO.
     *
     * @return the infoLogLevel
     */
    public String getInfoLogLevel() {
        return infoLogLevel;
    }

    /**
     * Error log level. Default = FATAL.
     *
     * @return the errorLogLevel
     */
    public String getErrorLogLevel() {
        return errorLogLevel;
    }

    /**
     * Info log file name.
     *
     * @return the infoLogFile
     */
    public String getInfoLogFile() {
        return infoLogFile;
    }

    /**
     *
     * @return
     */
    public String getFailedQueryFile() {
        return failedQueries;
    }

    /**
     * Error log file name.
     *
     * @return the errorLogFile
     */
    public String getErrorLogFile() {
        return errorLogFile;
    }

    /**
     * No of threads that will be created in the thread pool to process
     * profiles.
     *
     * @return the numOfChildren
     */
    public int getNumOfChildren() {
        return numOfChildren;
    }

    /**
     * Gets the connection timeout.
     *
     * @return the connection timeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Gets the reply timeout.
     *
     * @return the reply timeout
     */
    public int getReplyTimeout() {
        return replyTimeout;
    }
    /*
     * @return the Query Limit.
     */

    public int getQueryLimit() {
        return queryLimit;
    }

    /**
     * Gets the sleep time.
     *
     * @return the sleep time
     */
    public int getSleepTime() {
        return sleepTime;
    }

    public String getDbPoolName() {
        return dbPoolName;
    }

    public String getDbUserName() {
        return dbUserName;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public String getDbHost() {
        return dbHost;
    }

    public String getDbPort() {
        return dbPort;
    }

    public String getDbName() {
        return dbName;
    }

    public static String getPROPS_FILE() {
        return PROPS_FILE;
    }

    public String getTopic() {
        return topic;
    }

    public int getProcessedStatus() {
        return processed;
    }

    public int getProcessingStatus() {
        return processing;
    }

    public int getUnProcessedStatus() {
        return unProcessed;
    }

    public int getFinishedProcessedStatus() {
        return processingFinished;
    }

    /**
     * Get the max DB connections.
     *
     * @return the max DB connections
     */
    public int getDbMaxConnections() {
        return dbMaxConnections;
    }

    public int getAuthSuccessCode() {
        return authSuccessCode;
    }

    /**
     * A list of any errors that occurred while loading the properties.
     *
     * @return the loadErrors
     */
    public List<String> getLoadErrors() {
        return Collections.unmodifiableList(loadErrors);
    }

    public String getAckTopic() {
        return ackTopic;
    }

    public String getClientID() {
        return clientID;
    }

    public int getQoS() {
        return QoS;
    }

    public String getAccountsTopic() {
        return accountsTopic;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

}
