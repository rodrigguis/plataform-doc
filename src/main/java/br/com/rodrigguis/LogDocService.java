package br.com.rodrigguis;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogDocService {
    final static String TOPIC_PATTERN= "PLATAFORM.*";

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));

        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("****************************************************");
                System.out.println("Encontrei " + records.count() + " registros.");
                for (var messageRecords : records) {
                    System.out.println("Log messages topic: " + messageRecords.topic());
                    System.out.println("key:       " + messageRecords.key());
                    System.out.println("value:     " + messageRecords.value());
                    System.out.println("partition: " + messageRecords.partition());
                    System.out.println("record:    " + messageRecords.offset());
                }
            }
        }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogDocService.class.getName());

        return properties;
    }
}
