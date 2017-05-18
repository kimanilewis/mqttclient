/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argilaclient;

import argilaclient.db.MySQL;
import argilaclient.utils.Props;
import argilaclient.utils.Logging;
import argilaclient.utils.Constants;
import argilaclient.utils.CoreUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import static java.lang.Thread.sleep;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author crazywizard
 */
public final class ArgilaClient implements MqttCallback {

    /**
     * Log class instance.
     */
    private final Logging logging;
    /**
     * Properties instance.
     */
    /**
     * The string to append before the string being logged.
     */
    private final String logPreString;
    /**
     * System properties class instance.
     */
    private final Props props;
    /**
     * @param args the command line arguments
     */
    /**
     * The MySQL data source.
     */
    private final transient MySQL mysql;
    MqttClient client;
    String topic;
    String accountsTopic;
    String content = "";

    List<String> accountMap = new ArrayList<>();
//    String broker = "tcp://iot.eclipse.org:1883";
    String broker;
    String ackTopic;
    private String accountNumber;
    private String timeStamp;
    private String lastTimeStamp;
    private String clientID;
    private String MSISDN = null;
    private String accountBalance = null;
    private String availableTime = null;
    private String uniqueID = null;
    private ConnectionFactory connectionFactory;
    private com.rabbitmq.client.Connection con;
    private Channel ch;

    MemoryPersistence persistence = new MemoryPersistence();

    /**
     *
     * @param properties
     * @param log
     * @param mySQL
     * @throws InterruptedException
     */
    public ArgilaClient(final Props properties,
            final Logging log, MySQL mySQL) throws InterruptedException {

        props = properties;
        logging = log;
        mysql = mySQL;
        clientID = props.getClientID();
        broker = props.getBrokerUrl();
        ackTopic = props.getAckTopic();
        topic = props.getTopic();
        accountsTopic = props.getAccountsTopic();
        this.logPreString = "ArgilaClient | ";
        boolean onStart = false;
        List<String> loadErrors = properties.getLoadErrors();
        int sz = loadErrors.size();
        if (sz > 0) {
            log.info(logPreString + logPreString + "There were exactly "
                    + sz + " error(s) during the load operation...");
            for (String err : loadErrors) {
                log.fatal(logPreString + err);
            }
            log.info(logPreString + logPreString + "Unable to start daemon "
                    + "because " + sz + " error(s) occured during load.");
            System.exit(1);
        } else {
            log.info(logPreString
                    + "All required properties were loaded successfully");
        }
        subscribe(accountsTopic, clientID);
        Thread.currentThread();
        sleep(1000);
    }
//    public static void main(String[] args) {
//        ArgilaClient ac = new ArgilaClient(null, null, null);
//    }

    public void runClient() {
        for (int i = 0; i < accountMap.size(); i++) {
            try {
                logging.info(logPreString + "| " + Thread.currentThread().getId() + " | "
                        + "account available.... " + accountMap.toString());
                String account = accountMap.get(i);
                logging.info(logPreString
                        + "account Number: " + account);
                if (null != account) {
                    logging.info(logPreString
                            + "accounts is null  " + accountMap.size());
                    subscribe(account, clientID);
                    accountMap.remove(i);
                }
                sleep(2000);
            } catch (InterruptedException ex) {
                logging.error(logPreString + "InterruptedException caught "
                        + "while looping through accounts. Error: "
                        + ex.getMessage());
            }
        }
        logging.info(logPreString + Thread.currentThread().getId()
                + "| Connecting to broker  ..."
                + "\n ");
        subscribe(topic, clientID);

    }

    private void subscribe(String topic, String clientID) {

        try {
            logging.info(logPreString + "| "
                    + Thread.currentThread().getId() + " | "
                    + "Subscribing to topic: " + topic);

            // Construct the connection options object that contains connection parameters  
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            // Construct an MQTT blocking mode client
            client = new MqttClient(broker, clientID, persistence);
            // Set this wrapper as the callback handler
            client.setCallback(this);

            // Connect to the MQTT server
            System.out.println("Connecting to broker: " + broker);
            logging.info(logPreString + "Connecting to broker ..." + broker);
            client.connect(connOpts);
            System.out.println("Connected");
            logging.info(logPreString + "Connected and sucscribed to topic: "
                    + topic + " With QOS of:" + props.getQoS());

            // Subscribe to a topic
            System.out.println("Subscribe to topic:  " + "| "
                    + Thread.currentThread() + " | " + topic);
            client.subscribe(topic, props.getQoS());

        } catch (MqttException me) {
            logging.error(logPreString + "MqttException caught "
                    + "while connecting to broker. Error: "
                    + me.getMessage());
        }

    }

    @Override
    public void connectionLost(Throwable thrwbl) {
        System.out.println("connection lost: " + "| "
                + Thread.currentThread() + " | " + thrwbl.getMessage());
        logging.error(logPreString + "Connection lost while connecting to broker"
                + thrwbl.getMessage());
//        if (client.isConnected()) {
//            logging.error(logPreString + "Connection lost but connected"
//                    + thrwbl.getMessage());
//            runClient();
//        }

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println(topic + ":" + message);
        try {

            // Time stamp message
            String time = new SimpleDateFormat(Constants.DATE_FORMAT).format(new Date());
            // Set topic
            MqttTopic pubTopic = client.getTopic(ackTopic);
            logging.info(logPreString + "| " + Thread.currentThread().getId()
                    + " | topic: " + client.getTopic(topic)
                    + " | message received: " + message
                    + " At: " + time);
            System.out.println("Received at:" + time);

            if (message != null) {
                logging.info(logPreString + "| " + Thread.currentThread().getId()
                        + " | " + "message is not null");
                String jsonMessage = message.toString().trim();
                logging.info(logPreString + "| "
                        + Thread.currentThread().getId()
                        + " | " + "message is " + jsonMessage);
                JSONObject JsonRep = new JSONObject(jsonMessage);

                logging.info(logPreString + "| "
                        + Thread.currentThread().getId()
                        + " | " + "message is " + jsonMessage);
                logging.info(logPreString
                        + "message json object received: " + JsonRep);
                if (JsonRep.has("broadcastId")) {
                    uniqueID = JsonRep.getString("accountNumber");
                    if (JsonRep.has("accountNumber")) {
                        accountNumber = JsonRep.getString("accountNumber");
                        logging.info(logPreString + "accountNumber is:  " + accountNumber);
                    }
                    if (JsonRep.has("timestamp")) {
                        timeStamp = JsonRep.getString("timestamp");
                    }
                    logging.info(logPreString
                            + "time stamp is:  " + getLastTimeStamp());
                    if (JsonRep.has("availableTime")) {
                        availableTime = JsonRep.getString("availableTime");
                    }

                    if (JsonRep.has("accountBalance")) {
                        accountBalance = JsonRep.getString("accountBalance");
                    }
                    if (JsonRep.has("clientID")) {
                        clientID = JsonRep.getString("clientID");
                    }

                    insertNewRequest();
                } else {

//                        JSONObject object = accountArray.optJSONObject(i);
                    Iterator<String> iterator = JsonRep.keys();
//                        Iterator<String> iterator = object.keys();
                    while (iterator.hasNext()) {
                        accountNumber = iterator.next();
                        logging.info(logPreString + "| " + Thread.currentThread().getId()
                                + " | " + "account returned: " + accountNumber);
                        timeStamp = JsonRep.getString(accountNumber);
                        logging.info(logPreString + "| " + Thread.currentThread().getId()
                                + " | " + "time stamp returned: " + timeStamp);
                        Date timestampDate = new SimpleDateFormat(Constants.DATE_FORMAT).parse(timeStamp);
                        getLastTimeStamp();
                        if (lastTimeStamp != null) {
                            logging.info(logPreString + "| " + Thread.currentThread().getId()
                                    + "Latest time stored::" + lastTimeStamp);
                            Date localTimestampDate = new SimpleDateFormat(Constants.DATE_FORMAT).parse(lastTimeStamp);
                            logging.info(logPreString + "| " + Thread.currentThread().getId()
                                    + " | " + "account returned: " + accountNumber);
                            subscribe(accountNumber, clientID);
//                            if (localTimestampDate.after(timestampDate)) {
//                                logging.info(logPreString + "| " + Thread.currentThread().getId()
//                                        + " | " + "account returned: " + accountNumber);
//                                subscribe(accountNumber, clientID);
//                            } else {
//                                break;
//                            }

                        }
                    }

                }

            } else {
                logging.info(logPreString + "| " + Thread.currentThread().getId()
                        + " | " + "broker retuned an empty message");
            }

            logging.info(logPreString + "| "
                    + Thread.currentThread().getId()
                    + " | " + "Publishing to ACK topic1");
            // Create and configure a message
            String payload = "Received: "
                    + "| " + Thread.currentThread() + " | " + time;
            MqttMessage msg = new MqttMessage(uniqueID.getBytes());
            msg.setQos(props.getQoS());

            // Publish back to core
            MqttDeliveryToken token = null;
            logging.info(logPreString + "| "
                    + Thread.currentThread().getId() + " | " + "Publishing to ACK topic");
            System.out.println("Publishing to ACK topic");

            token = pubTopic.publish(msg);
            logging.info(logPreString + "| " + Thread.currentThread().getId()
                    + " | " + "Published to ACK topic:" + token.getMessage());
            // Wait for completion
            token.isComplete();
        } catch (ParseException | MqttException e) {
            logging.error(logPreString + "Exception cought while connecting to broker"
                    + e.getMessage());
        } catch (JSONException e) {
            logging.error(logPreString + "JSONException cought while receiving message from broker"
                    + e.getMessage());
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {
        System.out.println("Delivered " + imdt.hashCode());
        logging.info(logPreString + "| " + Thread.currentThread().getId()
                + " | topic: " + "Delivered " + imdt.hashCode());
        //update database as success 

    }

    /**
     * creates a new Core Request.
     */
    void insertNewRequest() {

        @SuppressWarnings("LocalVariableHidesMemberVariable")
        String logPreString = this.logPreString + "| "
                + Thread.currentThread().getId() + " | " + "insertNewRequest() | "
                + clientID + " | ";
        logging.info(logPreString + "| " + Thread.currentThread().getId()
                + "Creating new Core Request..."
                + this.clientID);
        String query = "INSERT INTO coreRequests (accountNumber, accountBalance, "
                + "availableTime, MSISDN, dateCreated) "
                + "VALUES (?, ?, ?, ?, ?) ";

        List<Object> params = new ArrayList<>();

        params.add(accountNumber);

        params.add(accountBalance);
        params.add(availableTime);

        params.add(MSISDN);

        params.add(timeStamp);
        logging.info(logPreString + "| " + Thread.currentThread().getId()
                + "Creating new Core Request..." + this.clientID);
        logging.info(logPreString + "| " + Thread.currentThread().getId()
                + "Creating new Core Request..." + this.clientID);
        String timestampDate = new SimpleDateFormat(Constants.DATE_FORMAT).format(timeStamp);
        try (Connection conn = mysql.getConnection();
                PreparedStatement st = conn.prepareStatement(query)) {
            st.setString(1, accountNumber);
            st.setString(2, accountBalance);
            st.setString(3, availableTime);
            st.setString(4, MSISDN);
            //st.setString(5, timestampDate);
            st.setTimestamp(5, (java.sql.Timestamp) java.sql.Timestamp.valueOf(timestampDate));
            st.executeUpdate();
        } catch (SQLException ex) {
            logging.error(logPreString + "An error occured"
                    + " while inserting a new core request"
                    + "..." + this.clientID + ex.getMessage());
            String failedQuery
                    = CoreUtils.prepareRowQueryFromPreparedPayload(query,
                            params);
            CoreUtils.updateFailedQueriesFile(Constants.FAILED_QUERIES_FILE,
                    failedQuery, logging);
        } catch (Exception ex) {
            logging.error(logPreString + "An error occured"
                    + " while inserting a new core request"
                    + "..." + this.clientID + ex.getMessage());
        }

    }

    /**
     *
     * @return
     */
    @SuppressWarnings("LocalVariableHidesMemberVariable")
    String getLastTimeStamp() {
        String logPreString = this.logPreString + "getLastTimeStamp() | ";
        String LatestTimestamp = null;
        String query = "SELECT timestamp FROM coreRequests "
                + "ORDER BY timestamp LIMIT 1";
        try (Connection con = mysql.getConnection();
                PreparedStatement st = con.prepareStatement(query)) {
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                LatestTimestamp = rs.getString("timestamp");
                lastTimeStamp = LatestTimestamp;
            }
        } catch (SQLException ex) {
            logging.error(logPreString + "Error getting latest timestamp : "
                    + ex.getMessage());
        }
        return lastTimeStamp;
    }

}
