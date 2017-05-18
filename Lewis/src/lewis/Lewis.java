/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lewis;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author crazywizard
 */
public class Lewis implements MqttCallback {

    /**
     * @param args the command line arguments
     */
    String topic = "broadcast/123";
    String content = "";
    int qos = 2;
    String broker = "tcp://iot.eclipse.org:1883";
    String clientId = "123";
    MemoryPersistence persistence = new MemoryPersistence();

    public static void main(String[] args) {
        Lewis argilaClients = new Lewis();
        argilaClients.runClients();
    }

    public void runClients() {

        try {
            // Construct the connection options object that contains connection parameters  
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            // Construct an MQTT blocking mode client
            MqttClient client = new MqttClient(broker, clientId, persistence);

            // Set this wrapper as the callback handler
            client.setCallback(this);

            // Connect to the MQTT server
            System.out.println("Connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println("Connected");

            // Subscribe to a topic
            System.out.println("Subscribe to topic: " + topic);
            client.subscribe(topic, qos);
        } catch (MqttException me) {
            System.out.println("Error thrown: " + me.getMessage());
        }
    }

    @Override
    public void connectionLost(Throwable thrwbl) {
        System.out.println("Connection Lost" + ":" + thrwbl);
        //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println(topic + ":" + message);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken imdt) {
        System.out.println("Delivery Complete" + ":" + imdt.toString());
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
