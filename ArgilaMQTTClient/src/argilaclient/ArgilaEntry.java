/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argilaclient;

import argilaclient.db.MySQL;
import argilaclient.utils.Logging;
import argilaclient.utils.Props;
import java.sql.SQLException;

/**
 *
 * @author lewie
 */
public class ArgilaEntry {

    /**
     * Logger for this application.
     */
    private static Logging log;
    /**
     * Loads system properties.
     */
    private static Props props;
    /**
     * The main run class.
     */
    private static ArgilaClient argilaClient;
    /**
     * The main run class.
     */
    private static rabbitMQClient rabbitClient;
    /**
     * The string to append before the string being logged.
     */
    private static String logPreString;
    /**
     * Initializes the MySQL connection pool.
     */
    private static MySQL mysql;

    /**
     * Private constructor.
     */
    private ArgilaEntry() {
    }

    /**
     * Test init().
     *
     * @throws java.lang.InterruptedException
     */
    public static void init() throws InterruptedException {

        try {
            props = new Props();
            log = new Logging(props);
            log.info("Host: " + props.getDbHost() + " Port: "
                    + props.getDbPort() + " Name: " + props.getDbName()
                    + " Username: " + props.getDbUserName() + " Password: **** "
                    + "Pool: " + props.getDbPoolName());
            mysql = new MySQL(props.getDbHost(), props.getDbPort(),
                    props.getDbName(), props.getDbUserName(),
                    props.getDbPassword(), props.getDbPoolName(), 100);
            log.info(" Initializing Recon Extract Matcher daemon...");
        } catch (ClassNotFoundException | InstantiationException |
                IllegalAccessException | SQLException ex) {
            log.fatal("Exception caught during initialization: " + ex);
            System.exit(-1);
        }

        argilaClient = new ArgilaClient(props, log, mysql);
//        rabbitClient = new rabbitMQClient(props, log, mysql);
    }

    /**
     * Main method.
     *
     * @param args command line arguments
     * @throws java.lang.InterruptedException
     */
//    @SuppressWarnings({"SleepWhileInLoop", "UseOfSystemOutOrSystemErr"})
//    public static void main(final String[] args) throws InterruptedException {
//        init();
//        log.info("Initialization Complete");
//        while (true) {
//            argilaClient.runClient();
//            log.info("Sleeping for "
//                    + props.getSleepTime() + " ms");
//            try {
//                Thread.sleep(props.getSleepTime());
//            } catch (InterruptedException ex) {
//                System.err.println(ex.getMessage());
//            }
//        }
//    }
    @SuppressWarnings({"SleepWhileInLoop", "UseOfSystemOutOrSystemErr"})
    public static void main(final String[] args) throws InterruptedException {
        init();
        log.info("Initialization Complete");
        argilaClient.runClient();
//        rabbitClient.run();
        log.info("Sleeping for "
                + props.getSleepTime() + " ms");
//            try {
//                Thread.sleep(props.getSleepTime());
//            } catch (InterruptedException ex) {
//                System.err.println(ex.getMessage());
//            }

    }
}
