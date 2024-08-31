package datasource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Duration;
import java.time.Instant;

public class KafkaDataSource extends RichSourceFunction<JsonNode> {
    private final String brokers;
    private final String topic;
    private final long allowedLatenessInMinutes;
    private org.apache.flink.connector.kafka.source.KafkaSource<String> kafkaSource;
    private ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper for JSON parsing
    private volatile boolean isRunning = true;

    public KafkaDataSource(String brokers, String topic, long allowedLatenessInMinutes) {
        this.brokers = brokers;
        this.topic = topic;
        this.allowedLatenessInMinutes = allowedLatenessInMinutes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize KafkaSource
        kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    @Override
    public void run(SourceContext<JsonNode> ctx) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                "KafkaSource"
        );

        // Collect the Kafka stream data to SourceContext
        kafkaStream.executeAndCollect().forEachRemaining(jsonString -> {
            try {
                // Parse JSON string to JsonNode
                JsonNode jsonNode = objectMapper.readTree(jsonString);
                synchronized (ctx.getCheckpointLock()) {
                    Instant timestamp = Instant.parse(jsonNode.get("timestamp").asText());
                    long eventTime = timestamp.toEpochMilli();

                    ctx.collectWithTimestamp(jsonNode, eventTime);
                    ctx.emitWatermark(new Watermark(eventTime - allowedLatenessInMinutes * 60 * 1000));
                }
            } catch (Exception e) {
                // Handle parsing errors
                System.err.println("Failed to parse record to JsonNode: " + e.getMessage());
            }
        });
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
