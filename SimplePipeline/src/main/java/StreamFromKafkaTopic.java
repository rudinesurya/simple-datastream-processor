import deserializationSchema.JsonNodeDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class StreamFromKafkaTopic {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final var kafkaSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        final var jsonStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        );

        jsonStream.print();

        env.execute("Kafka Source Example");
    }
}
