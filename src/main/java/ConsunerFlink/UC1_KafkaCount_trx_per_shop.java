package ConsunerFlink;

import commons.Commons;
import commons.serializeTuple2toString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


/**
 * className: ConsunerFlink.UC1_KafkaCount_trx_per_shop
 * trxStream: {"timestamp":1574168164819,"cc_id":"5188-1636-7534-2496","cc_type":"Visa","shop_id":4,"shop_name":"BioMarkt","fx":"EUR","fx_account":"CHF","amount_orig":59.09}
 * <p>
 * Aggregation on "shop_name"
 *
 * @author Marcel Daeppen
 * @version 2019/08/12 12:14
 */
@Slf4j
public class UC1_KafkaCount_trx_per_shop {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Commons.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("trx", new SimpleStringSchema(), properties));

        trxStream.print("input message: ");

        DataStream <Tuple2<String, Integer>> aggStream = trxStream
                .flatMap(new trxJSONDeserializer())
                // group by shop_name and sum their occurrences
                .keyBy(0)
                .sum(1);

        aggStream.print();

        // write the aggregated data stream to a Kafka sink
        aggStream.addSink(new FlinkKafkaProducer<>(
                Commons.EXAMPLE_KAFKA_SERVER,
                "result",
                new serializeTuple2toString()));

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka");
        JobID jobId = result.getJobID();
    }


    public static class trxJSONDeserializer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private transient ObjectMapper jsonParser;

        /**
         * Select the shop name from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get shop_name AND fx from JSONObject
            String shop_name = jsonNode.get("shop_name").toString();
            out.collect(new Tuple2<>(shop_name, 1));
        }
    }

}