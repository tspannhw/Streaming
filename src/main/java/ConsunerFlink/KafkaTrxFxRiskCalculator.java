package ConsunerFlink;

import commons.Commons;
import commons.trxfxJSONDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
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
 * @version 2019/08/26 16:08
 */
@Slf4j
public class KafkaTrxFxRiskCalculator {
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

        DataStream<String> trxfxStream = env.addSource(
                new FlinkKafkaConsumer<>("TrxFxCombined", new SimpleStringSchema(), properties));

        DataStream<Tuple11<Long, Integer, String, String, String, Double, String, String, Integer, Double, Long>> trxfxDeserializer = trxfxStream
                .flatMap(new trxfxJSONDeserializer());

        tableEnv.registerDataStream("trxfxTable", trxfxDeserializer, "trx_timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account, count, fx_rate, fx_timestamp");

        // dump
        Table sum1TrxFxTable = tableEnv.sqlQuery(
                "SELECT fx, fx_account, SUM(amount_orig) as amountOrig, SUM(amount_orig * fx_rate) as amountAccount FROM trxfxTable WHERE fx <> fx_account GROUP BY fx, fx_account");
        sum1TrxFxTable.printSchema();
        tableEnv.toRetractStream(sum1TrxFxTable, Row.class).print();


        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }
}