package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var dispatcher = new KafkaDispatcher();
        var key = UUID.randomUUID().toString();
        var value = key+ " 12121,2323232,9000";
        dispatcher.send("ECOMMERCE_NOVO_PEDIDO",key,value);
        var email = "Obrigado, estamos processando seu pedido";
        dispatcher.send("ECOMMERCE_SEND_EMAIL",key,email);
    }


}
