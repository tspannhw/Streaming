package commons;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

/**
 * {
 * "5183-7859-8786-3957":
 * {"cc_type":"Visa","shop_id":4,"fx":"USD","amount_orig":79.27,"fx_account":"CHF","cc_id":"5183-7859-8786-3957","shop_name":"BioMarkt","timestamp":1566803576745},
 * "USD":
 * {"fx_target":"CHF","fx":"USD","fx_rate":1.06,"timestamp":1566803577074}
 * }
 */


public class trxfxJSONDeserializer  implements FlatMapFunction<String, Tuple11<Long, Integer, String, String, String, Double, String, String, Integer, Double, Long>> {
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple11<Long, Integer, String, String, String, Double, String, String, Integer, Double, Long>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        // get shop_name AND fx from JSONObject
        Long timestamp = jsonNode.at("/trx").get("timestamp").asLong();
        Integer shop_id = jsonNode.at("/trx").get("shop_id").asInt();
        String shop_name = jsonNode.at("/trx").get("shop_name").toString();
        String cc_type = jsonNode.at("/trx").get("cc_type").toString();
        String cc_id = jsonNode.at("/trx").get("cc_id").toString();
        Double amount_orig = jsonNode.at("/trx").get("amount_orig").asDouble();
        String fx = jsonNode.at("/trx").get("fx").toString();
        String fx_account = jsonNode.at("/trx").get("fx_account").toString();
        Double fx_rate = jsonNode.at("/fx").get("fx_rate").asDouble();
        Long fx_timestamp = jsonNode.at("/fx").get("timestamp").asLong();
        out.collect(new Tuple11<>(timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account, 1, fx_rate, fx_timestamp));

    }
}