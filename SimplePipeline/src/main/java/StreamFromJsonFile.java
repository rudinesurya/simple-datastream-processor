import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import deserializationSchema.JsonNodeDeserializationSchema;


public class StreamFromJsonFile {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var filePath = StreamFromJsonFile.class.getClassLoader().getResource("test.json").getPath();

        final var fileStream = env.readTextFile(filePath);

        // Parse each line of JSON into a JsonNode
        final var jsonStream = fileStream.map(new MapFunction<String, JsonNode>() {
            private final JsonNodeDeserializationSchema deserializationSchema = new JsonNodeDeserializationSchema();

            @Override
            public JsonNode map(String value) throws Exception {
                return deserializationSchema.deserialize(value.getBytes());
            }
        });

        jsonStream.print();

        env.execute("Custom JSON Source Example");
    }
}
