package br.com.rodrigguis;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewDocTransfers {
    final static String TOPIC = "PLATAFORM_NEW_DOC";
    final static String TOPIC_EMAIL = "PLATAFORM_EMAIL_DOC";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(getProperties());
        var value = "132123, 675523, 1234";
        var messageRecord = new ProducerRecord<>(TOPIC, value, value);

        final Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando :::topic: " + data.topic() + " :::partition: " + data.partition() +
                    " :::offset: " + data.offset() + " :::timestamp: " + data.timestamp());
        };
        
        producer.send(messageRecord, callback).get();

        var email = "Hello, you received a new transfer DOC !!!";
        var emailRecord = new ProducerRecord<>(TOPIC_EMAIL, email, email);
        producer.send(emailRecord, callback).get();
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
