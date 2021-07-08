package br.com.rodrigguis;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailDocService {
    final static String TOPIC = "PLATAFORM_EMAIL_DOC";
    final static String GROUP_ID = EmailDocService.class.getSimpleName();

    public static void main(String[] args) {
        final var emailDocService = new EmailDocService();
        final var kafkaService = new KafkaService(GROUP_ID, TOPIC, emailDocService::parse);

        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> messageRecord) {
        System.out.println("****************************************************");
        System.out.println("Send email, checking for email service");
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
