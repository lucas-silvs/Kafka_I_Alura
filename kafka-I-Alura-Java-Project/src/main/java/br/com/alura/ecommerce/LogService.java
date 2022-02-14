package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String,String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while(true) {
            var records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                for (var record : records) {
                    System.out.println("\n");
                    System.out.println("Tópico: " + record.topic());
                    System.out.println("Chave: " + record.key());
                    System.out.println("Valor: " + record.value());
                    System.out.println("offset: " + record.offset());


                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {

                    }
                    System.out.println("Email Enviado");

                }
            }
            System.out.println("Registros vazios");
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }
}