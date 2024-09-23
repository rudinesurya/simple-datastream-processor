import datasource.KafkaSourceFunction;
import deserializationSchema.JsonNodeDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamFromKafkaTopic {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final var jsonSource = new KafkaSourceFunction<>("test", "localhost:9092", "my-group", new JsonNodeDeserializationSchema());
        final var jsonStream = env.addSource(jsonSource, TypeInformation.of(JsonNode.class));
        jsonStream.print();

        env.execute("Kafka Source Example");
    }
}
