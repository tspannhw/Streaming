package commons;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class serializeTuple2toString implements KeyedSerializationSchema<Tuple2<String, Integer>> {
    @Override
    public byte[] serializeKey(Tuple2 element) {
        return (null);
    }
    @Override
    public byte[] serializeValue(Tuple2 value) {

        String str = "{"+ value.getField(0).toString()
                + ":" + value.getField(1).toString() + "}";
        return str.getBytes();
    }
    @Override
    public String getTargetTopic(Tuple2 Tuple3) {
        // use always the default topic
        return null;
    }
}
