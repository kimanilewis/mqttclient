package argilaclient.utils;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Initialises the log files.
 *
 * @author <a href="kimanilewi@gmail.com">Lewis Kimani</a>
 */
@SuppressWarnings({ "FinalClass", "ClassWithoutLogger" })
public final class Logging {
    /**
     * Info log.
     */
    private static Logger infoLog;
    /**
     * Error log.
     */
    private static Logger errorLog;
    /**
     * Loaded system properties.
     */
    @SuppressWarnings("FieldMayBeFinal")
    private transient Props props;

    /**
     * Constructor.
     *
     * @param properties passed in loaded system properties
     */
    public Logging(final Props properties) {
        this.props = properties;
        initializeLoggers();
    }

    /**
     * Initialise the log managers.
     */
    @SuppressWarnings({
        "CallToThreadDumpStack",
        "UseOfSystemOutOrSystemErr",
        "CallToPrintStackTrace"
    })
    private void initializeLoggers() {
        infoLog = Logger.getLogger("infoLog");
        errorLog = Logger.getLogger("errorLog");

        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{yyyy MMM dd HH:mm:ss,SSS}: %p : %m%n");

        try {
            FileAppender faInfoLog = new FileAppender(layout,
                    props.getInfoLogFile(), true);
            FileAppender faErrorLog = new FileAppender(layout,
                        props.getErrorLogFile(), true);

            infoLog.addAppender(faInfoLog);
            errorLog.addAppender(faErrorLog);
        } catch (IOException ex) {
            System.err.println("Failed to initialize loggers... EXITING: "
                    + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }

        infoLog.setLevel(Level.toLevel(props.getInfoLogLevel()));
        errorLog.setLevel(Level.toLevel(props.getErrorLogLevel()));

        info("Just finished initializing Loggers...");
    }

    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public void info(final String message) {
        infoLog.info(message);
    }

    /**
     * Log debug messages.
     *
     * @param message the message content
     */
    public void debug(final String message) {
        infoLog.debug(message);
    }

    /**
     * Log error messages.
     *
     * @param message the message content
     */
    public void error(final String message) {
        errorLog.error(message);
    }

    /**
     * Log fatal error messages.
     *
     * @param message the message content
     */
    public void fatal(final String message) {
        errorLog.fatal(message);
    }
}
