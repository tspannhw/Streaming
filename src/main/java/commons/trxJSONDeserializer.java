package commons;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;


public class trxJSONDeserializer implements FlatMapFunction<String, Tuple9<Long, Integer, String, String, String, Double, String, String, Integer>> {
    private transient ObjectMapper jsonParser;

    /**
     * {"timestamp":1565604389166           long
     * ,"shop_id":0                         int
     * ,"shop_name":"Ums Eck"               string
     * ,"cc_type":"Revolut"                 string
     * ,"cc_id":"5179-5212-9764-8013"       string
     * ,"amount_orig":75.86                 Double
     * ,"fx":"CHF"                          string
     * ,"fx_account":"CHF"}                 string
     */
    @Override
    public void flatMap(String value, Collector<Tuple9<Long, Integer, String, String, String, Double, String, String, Integer>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        // get shop_name AND fx from JSONObject
        Long timestamp = jsonNode.get("timestamp").asLong();
        Integer shop_id = jsonNode.get("shop_id").asInt();
        String shop_name = jsonNode.get("shop_name").toString();
        String cc_type = jsonNode.get("cc_type").toString();
        String cc_id = jsonNode.get("cc_id").toString();
        Double amount_orig = jsonNode.get("amount_orig").asDouble();
        String fx = jsonNode.get("fx").toString();
        String fx_account = jsonNode.get("fx_account").toString();
        out.collect(new Tuple9<>(timestamp, shop_id, shop_name, cc_type, cc_id, amount_orig, fx, fx_account, 1));

    }
}