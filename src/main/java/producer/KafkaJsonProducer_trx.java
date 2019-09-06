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
            ));

    public static long sleeptime;


    public static void main(String[] args) throws Exception {


        if( args.length > 0 ) {
            setsleeptime(Long.parseLong(args[0]));
            System.out.println("sleeptime (ms): " + sleeptime);
        } else {
            System.out.println("no sleeptime defined - use default");
            setsleeptime(1000);
            System.out.println("default sleeptime (ms): " + sleeptime);
        }

        Producer<String, byte[]> producer = createProducer();
        try {
            for (int i = 0; i < 1000000; i++) {
                publishMessage(producer);
                Thread.sleep(sleeptime);
 //               System.out.println("used sleeptime: " + sleeptime);
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

        int i= random.nextInt(5);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("timestamp", System.currentTimeMillis());
        report.put("cc_id", "51" + (random.nextInt(89) + 10) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000));
        report.put("cc_type", transaction_card_type_list.get(random.nextInt(transaction_card_type_list.size())));
        report.put("shop_id", i);

        switch (i) {
            case 0:
                report.put("shop_name", "Tante_Emma" );
                break;
            case 1:
                report.put("shop_name", "Aus_der_Region" );
                break;
            case 2:
                report.put("shop_name", "Shop_am_Eck" );
                break;
            case 3:
                report.put("shop_name", "SihlCity" );
                break;
            case 4:
                report.put("shop_name", "BioMarkt" );
                break;
            default:
                System.out.println("i out of range");
        }

        report.put("fx", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("fx_account", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("amount_orig", (random.nextInt(8900) + 10) / 100.0);
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        KafkaJsonProducer_trx.sleeptime = sleeptime;
    }

}