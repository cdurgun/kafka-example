package com.cdurgun;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class KafkaEventProducer {

    public static void main(String[] args) throws Exception {
        // Kafka cluster bilgileri
        String bootstrapServers = "localhost:9092"; // Kafka broker adresi ve portu
        String topicName = "transactions"; // Göndereceğimiz Kafka topic adı

        // Kafka producer için gerekli ayarlar
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Kafka producer oluşturma
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Rastgele işlem oluşturmak için kullanılacak veri
        Random random = new Random();
        String[] customers = {"Alice", "Bob", "Charlie", "David", "Eve"};
        String[] transactionTypes = {"payment", "withdrawal", "deposit"};

        try {
            // 10 adet rastgele işlem gönderme
            for (int i = 0; i < 10; i++) {
                // Rastgele müşteri ve işlem türü seçimi
                String customer = customers[random.nextInt(customers.length)];
                String transactionType = transactionTypes[random.nextInt(transactionTypes.length)];
                double amount = random.nextDouble() * 1000;

                // Olay (event) oluşturma
                String event = "{ \"customer\": \"" + customer + "\", \"type\": \"" + transactionType + "\", \"amount\": " + amount + " }";

                // Kafka'ya olayı gönderme
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, event);
                producer.send(record);
                System.out.println("Gönderilen olay: " + event);

                // Küçük bir bekleme ekleyebiliriz, örneği okunması kolaylaştırır
                Thread.sleep(1000);
            }
        } finally {
            // Kafka producer kapatma
            producer.close();
        }
    }
}
