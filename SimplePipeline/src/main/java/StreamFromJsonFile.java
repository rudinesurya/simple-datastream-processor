import datasource.JSONSourceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import deserializationSchema.JsonNodeDeserializationSchema;


public class StreamFromJsonFile {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create and add the custom JSON source function
        final var jsonSource = new JSONSourceFunction<>("test.json", new JsonNodeDeserializationSchema());
        final var jsonStream = env.addSource(jsonSource, TypeInformation.of(JsonNode.class));
        jsonStream.print();

        env.execute("Custom JSON Source Example");
    }
}
