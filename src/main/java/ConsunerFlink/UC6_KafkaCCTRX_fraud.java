package ConsunerFlink;

import commons.Commons;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * className: ConsunerFlink.KafkaJoin2JsonStreams
 * trxStream: {"timestamp":1565604389166,"shop_name":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * <p>
 * Aggregation on "shop_name" & "fx"
 *
 * @author Marcel Daeppen
 * @version 2019/08/19 11:08
 */
@Slf4j
public class UC6_KafkaCCTRX_fraud {
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

        // get trx stream from kafka - topic "trx"
        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("trx", new SimpleStringSchema(), properties));

//        trxStream.print("trxStream");

        // deserialization of the received JSONObject into Tuple
        DataStream<Tuple3<String, Double, Integer>> aggStream = trxStream
                .flatMap(new trxJSONDeserializer())
                // group by "cc-id" and sum their instances
                .keyBy(0) // cc_id
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(2)
                // filter out if the cc_id is unique within the window {30 sec}. if the cc-id occurs several times && amount >= 40.00 send alarm event
                .filter(new FilterFunction<Tuple3<String, Double, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, Double, Integer> value) throws Exception {
                        return value.f1 >= 40 && value.f2 != 1;
                    }
                });

        aggStream.print("aggStream");

        // write the aggregated data stream to a Kafka sink
        aggStream.addSink(new FlinkKafkaProducer<>(
                Commons.EXAMPLE_KAFKA_SERVER,
                "fraudDedection",
                new serializeTuple3toString()));

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }

    public static class trxJSONDeserializer implements FlatMapFunction<String, Tuple3<String, Double, Integer>> {
        private transient ObjectMapper jsonParser;

        /**
         * Select the cc_id, aount_orig from the incoming JSON text as trx_fingerprint.
         */
        @Override
        public void flatMap(String value, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // build trx-fingerprint tuple
            String cc_id = jsonNode.get("cc_id").toString();
            Double amount_orig = jsonNode.get("amount_orig").asDouble();
            out.collect(new Tuple3<>(cc_id, amount_orig, 1));
        }
    }

    public static class serializeTuple3toString implements KeyedSerializationSchema<Tuple3<String, Double, Integer>> {
        @Override
        public byte[] serializeKey(Tuple3 element) {
            return (null);
        }

        @Override
        public byte[] serializeValue(Tuple3 value) {

            String str = "{" + value.getField(0).toString()
                    + ":" + value.getField(2).toString() + "}";
            return str.getBytes();
        }

        @Override
        public String getTargetTopic(Tuple3 tuple3) {
            // use always the default topic
            return null;
        }
    }
}