package datasource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class KafkaSourceFunction<T> extends RichSourceFunction<T> {
    private final String topic;
    private final String bootstrapServers;
    private final String groupId;
    private final DeserializationSchema<T> deserializationSchema;
    private volatile boolean isRunning = true;
    private org.apache.flink.connector.kafka.source.KafkaSource<T> kafkaSource;

    public KafkaSourceFunction(String topic, String bootstrapServers, String groupId, DeserializationSchema<T> deserializationSchema) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize KafkaSource
        kafkaSource = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializationSchema)
                .build();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Create a stream from the KafkaSource
        final var dataStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        );

        // Collect records
        try {
            dataStream.executeAndCollect().forEachRemaining(record -> {
                if (isRunning) {
                    ctx.collect(record);
                } else {
                    // Stop collecting if cancelled
                    throw new RuntimeException("Source function was cancelled.");
                }
            });
        } catch (Exception e) {
            // Handle exceptions gracefully
            System.err.println("Error while consuming records: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
