package commons;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public  class fxJSONDeserializer implements FlatMapFunction<String, Tuple4<Long, String, Double, String>> {

    private transient ObjectMapper jsonParser;

    /**
     * {"timestamp":1565604494202           Long
     * ,"fx":"EUR"                          String
     * ,"fx_rate":1.01}                     Double
     * ,"fx_target":"EUR"                   String
     */
    @Override
    public void flatMap(String value, Collector<Tuple4<Long, String, Double, String>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        // get values from JSONObject
        Long fx_timestamp = jsonNode.get("timestamp").asLong();
        String fx_org = jsonNode.get("fx").toString();
        Double fx_rate = jsonNode.get("fx_rate").asDouble();
        String fx_target = jsonNode.get("fx_target").toString();
        out.collect(new Tuple4<>(fx_timestamp, fx_org, fx_rate, fx_target));

    }
}