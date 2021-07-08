package br.com.rodrigguis;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewDocTransfers {
    static String TOPIC = "PLATAFORM_NEW_DOC";
    static String TOPIC_EMAIL = "PLATAFORM_EMAIL_DOC";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var kafkaDispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();

                var value = key + "675523, 1234";
                kafkaDispatcher.send(TOPIC, key, value);

                var email = "Hello, you received a new transfer DOC !!!";
                kafkaDispatcher.send(TOPIC_EMAIL, key, email);
            }
        }
    }
}
