package inprocess;

import commons.Commons;
import commons.fxJSONDeserializer;
import commons.trxJSONDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
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
public class KafkaStream2FlinkTable {
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

        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("trx", new SimpleStringSchema(), properties));

        DataStream<String> fxStream = env.addSource(
                new FlinkKafkaConsumer<>("fx", new SimpleStringSchema(), properties));

        DataStream<Tuple9<Long, Integer, String, String, String, Double, String, String, Integer>> trxDeserializer = trxStream
                .flatMap(new trxJSONDeserializer());

        DataStream<Tuple4<Long, String, Double, String>> fxDeserializer = fxStream
                .flatMap(new fxJSONDeserializer());


        tableEnv.registerDataStream("trxTable", trxDeserializer, "trx_timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account");

        tableEnv.registerDataStream("fxTable", fxDeserializer, "fx_timestamp, fx_org, fx_rate, fx_target");

        // count
        Table cntTrxTable = tableEnv.sqlQuery(
                "SELECT count(*) FROM trxTable ");
        cntTrxTable.printSchema();
//        tableEnv.toRetractStream(cntTrxTable, Row.class).print();


        Table cntFxTable = tableEnv.sqlQuery(
                "SELECT count(*) FROM fxTable ");
        cntFxTable.printSchema();
//        tableEnv.toRetractStream(cntFxTable, Row.class).print();

        // join
        Table join = tableEnv.sqlQuery(
                "SELECT * FROM trxTable INNER JOIN fxTable ON trxTable.fx = fxTable.fx_org AND trxTable.fx_account = fxTable.fx_target");

        join.printSchema();
        tableEnv.toRetractStream(join, Row.class).print();



        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }
}