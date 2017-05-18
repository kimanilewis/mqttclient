/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argilaclient;

import argilaclient.db.MySQL;
import argilaclient.utils.Constants;
import argilaclient.utils.CoreUtils;
import argilaclient.utils.Logging;
import argilaclient.utils.Props;

import java.io.IOException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import static java.lang.Thread.sleep;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author lewie
 */
public final class rabbitMQClient implements MqttCallback {

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

    Map<String, String> accountMap = new HashMap<>();
//    String broker = "tcp://iot.eclipse.org:1883";
    String broker;
    String ackTopic;
    private String accountNumber;
    private String timeStamp;
    private String clientID;
    private String MSISDN;
    private String accountBalance = null;
    private String availableTime = null;
    private String uniqueID = null;

    private ConnectionFactory connectionFactory;
    private Connection con;
    private Channel ch;

    MemoryPersistence persistence = new MemoryPersistence();

    // setup some variables which define where the MQTT broker is
    private final String host = "localhost"; //"69.64.82.36";
    private final int port = 1883;
    private final String brokerUrl = "tcp://" + host + ":" + port;
    private MqttConnectOptions conOpt;
    private ArrayList<MqttMessage> receivedMessages;

    private final int testDelay = 2000;
    private long lastReceipt;
    private boolean expectConnectionFailure;

    /**
     *
     * @param properties
     * @param log
     * @param mySQL
     * @throws InterruptedException
     */
    public rabbitMQClient(final Props properties,
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
        System.out.println("topic" + topic);
        try {
            setUpMqtt(); // initialise the MQTT connection

            System.out.println("topic" + topic);

            setUpAmqp(); // initialise the AMQP connection

        } catch (MqttException | IOException mqe) {
            mqe.getLocalizedMessage();
        }
        System.out.println("topic" + topic);
        subscribe(accountsTopic, clientID);
        Thread.currentThread();
        sleep(1000);
    }

    // override 10s limit
    private class MyConnOpts extends MqttConnectOptions {

        private int keepAliveInterval = 60;

        @Override
        public void setKeepAliveInterval(int keepAliveInterval) {
            this.keepAliveInterval = keepAliveInterval;
        }

        @Override
        public int getKeepAliveInterval() {
            return keepAliveInterval;
        }
    }

    public void setUpMqtt() throws MqttException {
        client = new MqttClient(brokerUrl, clientID);
        conOpt = new MyConnOpts();
        setConOpts(conOpt);
        receivedMessages = new ArrayList<>();
        expectConnectionFailure = false;

    }

    public void tearDownMqtt() throws MqttException {
        // clean any sticky sessions
        setConOpts(conOpt);
        client = new MqttClient(brokerUrl, clientID);
        try {
            client.connect(conOpt);
            client.disconnect();
        } catch (Exception _) {
        }
    }

    private void setUpAmqp() throws IOException {

        try {
            System.out.println("setting up AMQ");
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);
            System.out.println("all set");
            con = connectionFactory.newConnection();
            ch = con.createChannel();
            System.out.println("all set");
        } catch (TimeoutException ex) {
            System.out.println("error" + ex.getMessage());
        } catch (Exception ex) {
            System.out.println("error" + ex.getMessage());

        }
    }

    private void tearDownAmqp() throws IOException {
        con.close();
    }

    private void setConOpts(MqttConnectOptions conOpts) {
        // provide authentication if the broker needs it
        // conOpts.setUserName("guest");
        // conOpts.setPassword("guest".toCharArray());
        conOpts.setCleanSession(true);
        conOpts.setKeepAliveInterval(60);
    }

    private void publish(MqttClient client, String topicName,
            int qos, byte[] payload) throws MqttException {
        MqttTopic.validate(topicName, true);
        MqttTopic topic = client.getTopic(topicName);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos);
        MqttDeliveryToken token = topic.publish(message);
        token.waitForCompletion();
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
                    + topic + " With QOS of:" + props.getQoS() + " client:" + client.hashCode());

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
    public void connectionLost(Throwable cause) {
        if (!expectConnectionFailure) {
            System.out.println("Connection unexpectedly lost");
        }
    }

    /**
     * @param topic
     * @param message
     * @throws java.lang.Exception
     * @Override public void messageArrived(String topic, MqttMessage message)
     * throws Exception { lastReceipt = System.currentTimeMillis();
     * receivedMessages.add(message); System.out.println("message:" +
     * receivedMessages.toString()); }
     */
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
                if (JsonRep.has("accountNumber")) {
                    accountNumber = JsonRep.getString("accountNumber");
                    logging.info(logPreString + "accountNumber is:  " + accountNumber);
                    if (JsonRep.has("MSISDN")) {
                        MSISDN = JsonRep.getString("MSISDN");

                    }

                    if (JsonRep.has("timestamp")) {
                        timeStamp = JsonRep.getString("timestamp");
                    }
                    logging.info(logPreString
                            + "accountNumber is:  " + getLastTimeStamp());
                    if (JsonRep.has("availableTime")) {
                        availableTime = JsonRep.getString("availableTime");
                    }

                    if (JsonRep.has("accountBalance")) {
                        accountBalance = JsonRep.getString("accountBalance");
                    }
                    if (JsonRep.has("clientID")) {
                        clientID = JsonRep.getString("clientID");
                    }
                    if (JsonRep.has("uuid")) {
                        uniqueID = JsonRep.getString("uuid");
                    }

                    insertNewRequest();
                } else {

                    Date timestampDate = new SimpleDateFormat(Constants.DATE_FORMAT).parse(timeStamp);
                    while (getLastTimeStamp().before(timestampDate)) {
                        if (JsonRep.has("accountNumber")) {
                            accountNumber = JsonRep.getString("accountNumber");
                        }
                        if (JsonRep.has("timeStamp")) {
                            timeStamp = JsonRep.getString("timeStamp");
                        }
                        if (JsonRep.has("uuid")) {
                            uniqueID = JsonRep.getString("uuid");
                        }
                        accountMap.put("accountNumber", "254718668308");
                        accountMap.put("timestamp", timeStamp);
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
            MqttMessage msg = new MqttMessage(payload.getBytes());
            msg.setQos(props.getQoS());

            // Publish back to core
            MqttDeliveryToken token = null;
            logging.info(logPreString + "| " + Thread.currentThread().getId() + " | " + "Publishing to ACK topic");
            System.out.println("Publishing to ACK topic");

            token = pubTopic.publish(msg);
            logging.info(logPreString + "| " + Thread.currentThread().getId() + " | " + "Published to ACK topic:" + token.getMessage());
            // Wait for completion
            token.isComplete();
        } catch (JSONException | ParseException | MqttException e) {
            logging.error(logPreString + "Exception cought while connecting to broker"
                    + e.getMessage());
        }

    }

    /**
     *
     * @return
     */
    @SuppressWarnings("LocalVariableHidesMemberVariable")
    Date getLastTimeStamp() {
        String logPreString = this.logPreString + "getLastTimeStamp() | ";
        Date LatestTimestamp = null;
        String query = "SELECT timestamp FROM coreRequests "
                + "ORDER BY timestamp LIMIT 1";
        try (java.sql.Connection con = mysql.getConnection();
                PreparedStatement st = con.prepareStatement(query)) {
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                LatestTimestamp = rs.getDate("timestamp");
            }
        } catch (SQLException ex) {
            logging.error(logPreString + "Error getting latest timestamp : "
                    + ex.getMessage());
        }
        return LatestTimestamp;
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void run() {
        try {

            client.connect(conOpt);
            // subscribe(topic, clientID);  // subscribe to MQTT topic
            //publish(client, topic, 1, payload); // publish the MQTT message
            client.disconnect();
            Thread.sleep(testDelay);
            System.out.println("queue" + ch.toString());
            String queue = ch.queueDeclare().getQueue();//"Broadcast"; 

            ch.queueBind(queue, "amq.topic", topic);
            GetResponse response = ch.basicGet(queue, true); // get the AMQ response
//            System.out.println(new String(response.getBody()));
            System.out.println("response:" + response);
            logging.info(logPreString + "| " + Thread.currentThread().getId() + " | " + "rabbit channel: " + queue);
//            tearDownAmqp(); // cleanup AMQP resources
//            tearDownMqtt(); // cleanup MQTT resources

        } catch (MqttException | InterruptedException | IOException mqe) {
            mqe.getLocalizedMessage();
        }
    }

    /**
     * creates a new Core Request.
     */
    void insertNewRequest() {

        @SuppressWarnings("LocalVariableHidesMemberVariable")
        String logPreString = this.logPreString + "| " + Thread.currentThread().getId() + " | " + "insertNewRequest() | "
                + clientID + " | ";
        logging.info(logPreString + "| " + Thread.currentThread().getId() + "Creating new Core Request..."
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
        logging.info(logPreString + "| " + Thread.currentThread().getId() + "Creating new Core Request..."
                + this.clientID);
        logging.info(logPreString + "| " + Thread.currentThread().getId() + "Creating new Core Request..."
                + this.clientID);
        String timestampDate = new SimpleDateFormat(Constants.DATE_FORMAT).format(timeStamp);
        try (java.sql.Connection con = mysql.getConnection();
                PreparedStatement st = con.prepareStatement(query)) {
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

}
