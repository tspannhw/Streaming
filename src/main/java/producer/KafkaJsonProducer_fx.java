package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import commons.Commons;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;



public class KafkaJsonProducer_fx {
    private static final String BOOTSTRAP_SERVERS = Commons.EXAMPLE_KAFKA_SERVER;
    private static final String TOPIC = "fx";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();


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
//                System.out.println("used sleeptime: " + sleeptime);

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

        int i= random.nextInt(8);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("timestamp", System.currentTimeMillis());

        switch (i) {
            case 0:
                report.put("fx", "CHF");
                report.put("fx_rate", 1.00);
                report.put("fx_target", "CHF");
                break;
            case 1:
                report.put("fx", "CHF");
                report.put("fx_target", "USD");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            case 2:
                report.put("fx", "CHF");
                report.put("fx_target", "EUR");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            case 3:
                report.put("fx", "EUR");
                report.put("fx_rate", 1.00);
                report.put("fx_target", "EUR");
                break;
            case 4:
                report.put("fx", "EUR");
                report.put("fx_target", "USD");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            case 5:
                report.put("fx", "EUR");
                report.put("fx_target", "CHF");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            case 6:
                report.put("fx", "USD");
                report.put("fx_rate", 1.00);
                report.put("fx_target", "USD");
                break;
            case 7:
                report.put("fx", "USD");
                report.put("fx_target", "CHF");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            case 8:
                report.put("fx", "USD");
                report.put("fx_target", "EUR");
                report.put("fx_rate", (random.nextInt(20) + 90) / 100.0);
                break;
            default:
                System.out.println("i out of range");

        }
        return report;
    }

    private static void setsleeptime(long sleeptime) {
        KafkaJsonProducer_fx.sleeptime = sleeptime;
    }

}