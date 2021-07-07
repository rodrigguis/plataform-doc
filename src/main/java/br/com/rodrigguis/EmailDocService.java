package br.com.rodrigguis;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailDocService {
    final static String TOPIC = "PLATAFORM_EMAIL_DOC";

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros.");
                for (var messageRecords : records) {
                    System.out.println("****************************************************");
                    System.out.println("Send email, checking for email service");
                    System.out.println("key:       " + messageRecords.key());
                    System.out.println("value:     " + messageRecords.value());
                    System.out.println("partition: " + messageRecords.partition());
                    System.out.println("record:    " + messageRecords.offset());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }

                    System.out.println("Order processed . . .");
                }
            }
        }
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailDocService.class.getName());

        return properties;
    }
}
