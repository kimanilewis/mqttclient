package argilaclient.utils;

/**
 * Constants class.
 *
 * @author lewis Kimani
 */
@SuppressWarnings({"FinalClass", "ClassWithoutLogger"})
public final class Constants {

    /**
     * Private constructor.
     */
    private Constants() {
    }
    /**
     * Maximum size of a log file.
     */
    public static final String MAX_LOG_FILE_SIZE = "500GB";
    /**
     * Maximum number of log files.
     */
    public static final int MAX_NUM_LOGFILES = 4;
    public static final String FAILED_QUERIES_FILE = "FAILED_QUERIES.TXT";
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String UPDATE_ID = "update";
    public static final int RETRY_COUNT = 1;
    public static final int ACTIVE = 1;

}
