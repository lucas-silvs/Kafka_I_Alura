package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FraudeDetectorService {
    public static void main(String[] args) {
        var fraudeDetectorService = new FraudeDetectorService();
        var kafkaService = new KafkaService<Order>(FraudeDetectorService.class.getSimpleName(),"ECOMMERCE_NOVO_PEDIDO", fraudeDetectorService::parse,Order.class, Map.of());
        kafkaService.run();
    }



    private  void parse(ConsumerRecord<String,Order> record){
        System.out.println("\n");
        System.out.println("processando pedidos, necessário criar validação de fraude");
        System.out.println("Chave: " + record.key());
        System.out.println("Valor: " + record.value());
        System.out.println("Tópico: " + record.topic());
        System.out.println("offset: " + record.offset());


        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }
        System.out.println("Pedido processado");
    }


}
