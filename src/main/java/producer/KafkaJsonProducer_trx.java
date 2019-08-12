package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import commons.Commons;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

import static java.util.Collections.unmodifiableList;


public class KafkaJsonProducer_trx {
    private static final String BOOTSTRAP_SERVERS = Commons.EXAMPLE_KAFKA_SERVER;
    private static final String TOPIC = "trx";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    private static final List<String> transaction_card_type_list = unmodifiableList(Arrays.asList(
            "Visa", "MasterCard", "Maestro", "AMEX", "Diners Club", "Revolut"));
    private static final List<String> transaction_currency_list = unmodifiableList(Arrays.asList(
            "USD", "EUR", "CHF"));

    private static final List<String> shop_list = unmodifiableList(Arrays.asList(
            "Tante_Emma", "Aus_der_Region", "Shop_am_Eck", "SihlCity", "BioMarkt"));

    public static void main(String[] args) throws Exception {
        Producer<String, byte[]> producer = createProducer();
        try {
            for (int i = 0; i < 1000000; i++) {
                publishMessage(producer);
                Thread.sleep(random.nextInt(5000));
            }
        } finally {
            producer.close();
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "md");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        String key = UUID.randomUUID().toString();

        ObjectNode messageJsonObject = JsonOnject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, key, valueJson);

        RecordMetadata md = producer.send(record).get();
        System.out.println("Published " + md.topic() + "/" + md.partition() + "/" + md.offset()
                + " (key=" + key + ") : " + messageJsonObject);
    }

    // build random json object
    private static ObjectNode JsonOnject() {
        ObjectNode report = objectMapper.createObjectNode();
        report.put("timestamp", System.currentTimeMillis());
        report.put("shop_id", random.nextInt(5));
        report.put("shop_name", shop_list.get(random.nextInt(shop_list.size())));
        report.put("cc_type", transaction_card_type_list.get(random.nextInt(transaction_card_type_list.size())));
        report.put("cc_id", "51" + (random.nextInt(89) + 10) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000));
        report.put("amount_orig", (random.nextInt(8900) + 10) / 100.0);
        report.put("fx", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("fx_account", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        return report;
    }

}