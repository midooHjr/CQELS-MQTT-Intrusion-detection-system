package cqelsplus.MQTT;

import cqelsplus.JENA.JENAOntology;
import cqelsplus.launch.MQTTStream;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;

import static cqelsplus.engine.RDFStream.n;


public class MQTTHelper{
    private MQTT mqtt;
    private CallbackConnection connection;
    private MQTTStream stream;

    private JENAOntology ontology;

    public MQTTHelper(MQTTStream stream, JENAOntology ontology) {
        mqtt = new MQTT();
        try {
            mqtt.setHost("localhost", 1883);

        }catch (URISyntaxException e){
            e.printStackTrace();
            System.exit(-1);
        }

        connection = mqtt.callbackConnection();
        this.stream = stream;
        this.ontology = ontology;
    }


    public void setNewListner() {
        connection.listener(new Listener() {
            @Override
            public void onConnected() {
                System.out.println("MQTT : Connected successfully");
            }

            @Override
            public void onDisconnected() {
                System.out.println("MQTT : Connection closed");
            }

            @Override
            public void onPublish(UTF8Buffer utf8Buffer, Buffer buffer, Runnable runnable) {
                runnable.run();

                //System.out.println("MQTT : New message published in topic \"" + utf8Buffer + "\" : ");
                String txtOnly = buffer.toString().split(":")[1].substring(1);

                txtOnly=txtOnly.replace('=',':');
                System.out.println("Txt only : "+txtOnly);

                try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("/home/midoo/tmp/cqels/cqels_data/stream/tmp",false), "utf-8"))) {
                    writer.write(txtOnly);
                }catch (Exception e) {
                    e.printStackTrace();
                }
                //stream.setMessage(txtOnly+"\n");
                stream.run();

                // add stream data to JENA ontology (optional)
                ontology.addStatementToDataSet(txtOnly.split("\n"));
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("MQTT : Connection failed");
                connection.suspend();
            }
        });
    }

    public void establishConnection(){
            connection.connect(new Callback<Void>() {
                @Override
                public void onSuccess(Void aVoid) {
                    System.out.println("MQTT : Connection established successfully");
                }

                @Override
                public void onFailure(Throwable throwable) {
                    try {
                        System.out.println("MQTT : Connection failure");
                        throw connection.failure();
                    } catch (Throwable throwable1) {
                        throwable1.printStackTrace();
                    }
                }
            });
    }

    public void subscribeToTopic(String topic){
        Topic[] topics = {new Topic(topic,QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics, new Callback<byte[]>() {
            @Override
            public void onSuccess(byte[] bytes) {
                System.out.println("MQTT : Subscribed to topic successfully");
            }

            @Override
            public void onFailure(Throwable throwable) {
                try {
                    System.out.println("MQTT : Connection failure");
                    throw connection.failure();
                } catch (Throwable throwable1) {
                    throwable1.printStackTrace();
                }
            }
        });
    }

    public void publishMessage(String topic, String message){
        connection.publish(topic, message.getBytes(), QoS.AT_LEAST_ONCE, false, new Callback<Void>() {
            public void onSuccess(Void v) {
                System.out.println("MQTT: Message published successuflly");
            }
            public void onFailure(Throwable value) {
                System.out.println("MQTT: Message publication failed");

                connection.suspend(); // publish failed.
            }
        });
    }

    public void closeConnection(){
        connection.disconnect(new Callback<Void>() {
            public void onSuccess(Void v) {
                System.out.println("Disconnected successfully");
            }
            public void onFailure(Throwable value) {
                System.out.println("Disconnection failed");
            }
        });
    }


}