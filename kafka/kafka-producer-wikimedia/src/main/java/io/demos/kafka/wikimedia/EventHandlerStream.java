package io.demos.kafka.wikimedia;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandlerStream implements EventHandler {

    String topic;
    KafkaProducer<String, String> kafkaProducer;
    private  static  final Logger log =  LoggerFactory.getLogger(EventHandlerStream.class.getSimpleName());

    public EventHandlerStream(String topic, KafkaProducer<String, String> producer){
        this.topic = topic;
        this.kafkaProducer = producer;
    }
    @Override
    public void onOpen(){
        // do nothing
    }

    @Override
    public void onClosed(){
            kafkaProducer.close();
    }
    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
            // test:
            log.info(messageEvent.getData());
            kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment){
        // do nothing
    }

    @Override
    public void onError(Throwable t) {
        log.error("error while sending data: ", t);
    }
}
