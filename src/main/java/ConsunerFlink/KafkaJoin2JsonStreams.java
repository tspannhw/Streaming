package ConsunerFlink;

import com.alibaba.fastjson.JSONObject;
import commons.Commons;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * className: ConsunerFlink.KafkaJoin2JsonStreams
 * trxStream: {"timestamp":1565604389166,"shop_id":0,"shop_name":"Ums Eck","cc_type":"Revolut","cc_id":"5179-5212-9764-8013","amount_orig":75.86,"fx":"CHF","fx_account":"CHF"}
 * fxStream: {"timestamp":1565604494202,"fx":"EUR","fx_rate":1.01}
 * <p>
 * DataStream<String> joinedString = trx.join(fx)
 * kafka: topic1
 * {"EUR":{"fx":"EUR","fx_rate":0.9,"timestamp":1565604610729},"5130-2220-4900-6727":{"cc_type":"Visa","shop_id":4,"fx":"EUR","amount_orig":86.82,"fx_account":"EUR","cc_id":"5130-2220-4900-6727","shop_name":"Ums Eck","timestamp":1565604610745}}
 *
 * @author Marcel Daeppen
 * @version 2019/08/12 12:14
 */
@Slf4j
public class KafkaJoin2JsonStreams {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Commons.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //it is necessary to use IngestionTime, not EventTime. during my running this program
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<String> trxStream = env.addSource(
                new FlinkKafkaConsumer<>("trx", new SimpleStringSchema(), properties));

        System.out.println("DataStream - trx");

        DataStream<String> fxStream = env.addSource(
                new FlinkKafkaConsumer<>("fx", new SimpleStringSchema(), properties));

        System.out.println("DataStream - fx");

        DataStream<JSONObject> trx =
                trxStream.flatMap(new Tokenizer());

        DataStream<JSONObject> fx =
                fxStream.flatMap(new Tokenizer());

        DataStream<String> joinedString = trx.join(fx)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
                .apply((JoinFunction<JSONObject, JSONObject, String>) (first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put("trx", first);
                    joinJson.put("fx", second);

                    // for debugging: print out
//                    System.out.println("trx data: " + first);
//                    System.out.println("fx data: " + second);
                    return joinJson.toJSONString();
                });


        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                Commons.EXAMPLE_KAFKA_SERVER,
                "TrxFxCombined",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        joinedString.addSink(myProducer);


        // emit result
        if (parameterTool.has("output")) {
            joinedString.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            joinedString.print();
        }

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }


    public static final class Tokenizer implements FlatMapFunction<String, JSONObject> {

        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                JSONObject json = JSONObject.parseObject(value);
                out.collect(json);
            } catch (Exception ex) {
                System.out.println(value + ex);
            }
        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_account from fxStream
            final String str = (String) value.get("fx") + "_" + (String) value.get("fx_account");
// for debugging: print out
//            System.out.println("trx: " + str);
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            // select fx && fx_target from fxStream
            final String str = (String) value.get("fx") + "_" + (String) value.get("fx_target");
// for debugging: print out
//           System.out.println("fx: " + str);
            return str;
        }
    }
}