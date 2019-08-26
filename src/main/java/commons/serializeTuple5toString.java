package commons;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class serializeTuple5toString implements KeyedSerializationSchema<Tuple5<String, String, String, String, Double>> {
    @Override
    public byte[] serializeKey(Tuple5 element) {
        return (null);
    }
    @Override
    public byte[] serializeValue(Tuple5 value) {

        String str = "{"+ value.getField(0).toString()
                + ":" + value.getField(1).toString()
                + ":" + value.getField(2).toString()
                + ":" + value.getField(4).toString() + "}";
        return str.getBytes();
    }
    @Override
    public String getTargetTopic(Tuple5 tuple5) {
        // use always the default topic
        return null;
    }
}