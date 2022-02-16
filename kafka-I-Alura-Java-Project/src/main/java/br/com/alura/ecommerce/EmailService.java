package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class EmailService  {
    public static void main(String[] args) {
        var emailService = new EmailService();
        var kafkaService = new KafkaService<Email>(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL",emailService::parse, Email.class,  Map.of());
        kafkaService.run();

    }

    private void  parse(ConsumerRecord<String,Email> record){
        System.out.println("\n");
        System.out.println("Log");
        System.out.println("Chave: " + record.key());
        System.out.println("Valor: " + record.value());
        System.out.println("Tópico: " + record.topic());
        System.out.println("offset: " + record.offset());
        try {
            Thread.sleep(1000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }

    }


}
