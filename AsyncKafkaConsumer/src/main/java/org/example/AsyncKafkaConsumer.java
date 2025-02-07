package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;

public class AsyncKafkaConsumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        String topic = "my-topic";
        consumer.subscribe(Collections.singletonList(topic));

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        try {
            while (true) {
                var records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    executorService.submit(() -> {
                        processMessage(record);
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            executorService.shutdown();
        }
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        try {
            // Имитируем долгую операцию
            System.out.println("Start processing message: " + record.value());
            Thread.sleep(2000);
            System.out.println("Processed message: " + record.value());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Error processing message: " + record.value());
        }
    }
}
