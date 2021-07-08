package br.com.rodrigguis;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDocService {
    static String TOPIC = "PLATAFORM_NEW_DOC";
    static String GROUP_ID = FraudDocService.class.getSimpleName();

    public static void main(String[] args) {
        var fraudDocService = new FraudDocService();
        try (final var kafkaService = new KafkaService(GROUP_ID, TOPIC, fraudDocService::parse)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> messageRecord) {
        System.out.println("****************************************************");
        System.out.println("Processing new Order, checking for fraud");
        System.out.println("key:       " + messageRecord.key());
        System.out.println("value:     " + messageRecord.value());
        System.out.println("partition: " + messageRecord.partition());
        System.out.println("record:    " + messageRecord.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        System.out.println("Order processed . . .");
    }
}
