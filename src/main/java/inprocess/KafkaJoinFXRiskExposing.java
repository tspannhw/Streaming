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
 * className:
 * trxStream: {"timestamp":1566372865956,"shop_id":4,"shop_name":"Shop_am_Eck","cc_type":"AMEX","cc_id":"5118-5303-1207-5269","amount_orig":37.27,"fx":"CHF","fx_account":"EUR"}
 * fxStream: {"timestamp":1566372727389,"fx":"CHF","fx_rate":1.04,"fx_target":"EUR"}
 *
 * @author Marcel Daeppen
 * @version 2019/08/19 11:08
 */
@Slf4j
public class KafkaJoinFXRiskExposing {
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

        DataStream<Tuple9<Long, Integer, String, String, String, Double, String, String, Integer>> trxDeserializer = trxStream
                .flatMap(new trxJSONDeserializer());
        trxDeserializer.print();

        // get trx stream from kafka - topic "fx"
        DataStream<String> fxStream = env.addSource(
                new FlinkKafkaConsumer<>("fx", new SimpleStringSchema(), properties));

        DataStream<Tuple4<Long, String, Double, String>> fxDeserializer = fxStream
                .flatMap(new fxJSONDeserializer());
        fxDeserializer.print();



        Table trxTable = tableEnv.fromDataStream(trxDeserializer, "timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account");

        Table fxTable = tableEnv.fromDataStream(fxDeserializer, "fx_timestamp, fx_org, fx_rate, fx_target");


        // joining the two tables trxTable & fxTable into one
        Table joinedTable = trxTable.join(fxTable)
                .where("fx = fx_org && fx_account = fx_target");

//        tableEnv.toRetractStream(joinedTable, Row.class).print();

        // sum "amount_orig" per currency conversion ("CHF" > "EUR")
        Table groupedTable = joinedTable
                .groupBy("fx_org, fx_target")
                .select("fx_org, fx_target, SUM(amount_orig)");

        tableEnv.toRetractStream(groupedTable, Row.class).print();
/*
        Table risk = groupedTable
                .where("fx_org <> fx_target");
        tableEnv.toRetractStream(risk, Row.class).print();
*/

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }


}