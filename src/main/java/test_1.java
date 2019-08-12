import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * className: KafkaJoinJsonEqualWindow
 * <p>
 * iot-tempA topic输入内容类似， {"deviceId":"A", "A1031":21}、
 * iot-tempB topic输入内容类似， {"deviceId":"B","B1031":20}、 {"deviceId":"B","B1031":21}
 * <p>
 * 然后将DataStream<JSONObject>转换为DataStream<String>， 最后将结果写入到kafka中，
 * 结果为以下内容。 每一行为一条kafka消息
 * kafka最终topic1中会出现
 * {"A":{"A1031":21,"deviceId":"A"},"B":{"B1031":20,"deviceId":"B"}}
 * {"A":{"A1031":21,"deviceId":"A"},"B":{"B1031":21,"deviceId":"B"}}
 *
 * @author EricYang
 * @version 2019/05/28 14:50
 */
@Slf4j
class test_1 {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //createRemoteEnvironment(String host, int port, String... jarFiles)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "md");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //it is necessary to use IngestionTime, not EventTime. during my running this program
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> steam_trx = env.addSource(
                new FlinkKafkaConsumer<String>("trx", new SimpleStringSchema(), properties));
//      steam_trx.print();
//      {"timestamp":1565280308791,"shop_id":1,"shop_name":"SihlCity","cc_type":"Diners Club","cc_id":"5171-2647-5878-9263","amount_orig":24.79,"fx":"CHF","fx_account":"EUR"}

        DataStream<Tuple8<String, String, String, String, String, String, String, String>> Tuple8trx = steam_trx.map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            String timestamp = jsonObject.getString("timestamp");
            String fx = jsonObject.getString("fx");
            String shop_id = jsonObject.getString("shop_id");
            String shop_name = jsonObject.getString("shop_name");
            String cc_type = jsonObject.getString("cc_type");
            String cc_id = jsonObject.getString("cc_id");
            String amount_orig = jsonObject.getString("amount_orig");
            String fx_account = jsonObject.getString("fx_account");
            return Tuple8.of(timestamp, fx, shop_id, shop_name, cc_type, cc_id, amount_orig, fx_account);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING));
        Tuple8trx.print();


        DataStream<String> stream_fx = env.addSource(
                new FlinkKafkaConsumer<String>("fx", new SimpleStringSchema(), properties));
//      stream_fx.print();
//      {"timestamp":1565280909760,"fx":"CHF","fx_rate":0.98}

        DataStream<Tuple3<String, String, String>> Tuple3fx = stream_fx.map(s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            String timestamp = jsonObject.getString("timestamp");
            String fx = jsonObject.getString("fx");
            String fx_rate = jsonObject.getString("fx_rate");
            return Tuple3.of(timestamp, fx, fx_rate);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));
        Tuple3fx.print();

/*
        //实时流Join Es表的流
        DataStream<Tuple8<Tuple2<String, String>, String, Integer>> finalResult = Tuple3fx.join(dimTable).where(new FirstKeySelector()).equalTo(new SecondKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000))).apply(
                        new JoinFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<Tuple2<String, String>, String>, Tuple3<Tuple2<String, String>, String, Integer>>() {
                            @Override
                            public Tuple3<Tuple2<String, String>, String, Integer> join(Tuple2<Tuple2<String, String>, Integer> first, Tuple2<Tuple2<String, String>, String> second) throws Exception {
                                return Tuple3.of(first.f0, second.f1, first.f1);
                            }
                        }
                );

*/



/*
        DataStream<JSONObject> countsA =
                aStream.flatMap(new Tokenizer());

        DataStream<JSONObject> countsB =
                bStream.flatMap(new Tokenizer());


        DataStream<String> countsString = countsA.join(countsB)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .apply ((JoinFunction<JSONObject, JSONObject, String>) (first, second) -> {
                    JSONObject joinJson = new JSONObject();
                    joinJson.put(first.getString("fx"), first);
                    joinJson.put(second.getString("fx"), second);

                    return  joinJson.toJSONString();
                });
*/
/*
        countsString.print();
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                KAFKA_BROKERS,
                "topic1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        countsString.addSink(myProducer);
*/
        // emit result
/*        if (parameterTool.has("output")) {
            countsString.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            countsString.print();
        }
*/
        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }

/*
    public static final class Tokenizer implements FlatMapFunction<String, JSONObject> {

        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                JSONObject json = JSONObject.parseObject(value);
                out.collect(json);
                System.out.println(value);
                System.out.println(json);
            } catch (Exception ex) {
                System.out.println( value + ex);
            }

        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            final String str = "A";
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            final String str = "A";
            return str;
        }
    }
*/
}
