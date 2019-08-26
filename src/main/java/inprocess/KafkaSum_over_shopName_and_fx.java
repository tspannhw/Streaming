package inprocess;

import commons.Commons;
import commons.trxJSONDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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
public class KafkaSum_over_shopName_and_fx {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Commons.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // get trx stream from kafka - topic "trx"
        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("trx", new SimpleStringSchema(), properties));

        // deserialization of the received JSONObject into Tuple
        DataStream<Tuple9<Long, Integer, String, String, String, Double, String, String, Integer>> trxDeserializer = trxStream
                .flatMap(new trxJSONDeserializer());
//        trxDeserializer.print();

/*
        DataStream<Tuple4<String, String, Double, Double>> aggStream = trxStream
                .flatMap(new JSONDeserializer())
                // group by shop_name AND fx and sum their occurrences
                .keyBy(0, 1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(3);
*/


        Table table = tableEnv.fromDataStream(trxDeserializer, "timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account");

        Table result1 = table
     //           .window(Tumble.over("10.seconds").on("timestamp").as("w"))
                .groupBy("shop_name, fx")
                .select("shop_name, fx, sum(amount_orig)");

        tableEnv.toRetractStream(result1, Row.class).print();


        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }


}