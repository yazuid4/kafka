package io.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Try;

import java.util.Properties;

public class ProducerWithCallbacks {
    private  static  final Logger log =  LoggerFactory.getLogger(ProducerWithCallbacks.class.getSimpleName());
    public static void main(String[] args) {
        log.info("start producer execution:");
        // properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 2; i++){
            for(int j = 0; j < 3; j++){
                String topic = "demo_java_2";
                String key = "key_" + j;
                String value = "Hello World" + j;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord(topic, key, value);
                // send
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executed every time a record successfully sent
                        if (e == null){
                            log.info("data are sent - " +
                                    "Key: " + key + " - " +
                                    "Partition: " + metadata.partition() + "\n" );
                        }
                        else{
                            log.error("Error while producing data, " , e);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            }catch(InterruptedException e){
                    e.printStackTrace();
            }
        }
        // flush & close
        // producer.flush();
        producer.close();
    }
}
