import deserializationSchema.JsonNodeDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;


public class StreamFromKafkaTopic {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing with a checkpoint interval of 5000ms (5 seconds)
        env.enableCheckpointing(5000); // Interval in milliseconds
        // Optional: Configure more advanced settings for checkpointing
        env.getCheckpointConfig().setCheckpointTimeout(60000); // Timeout after 60 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000); // 1 second pause between checkpoints
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // Allow only one checkpoint at a time
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // Allow 3 failed checkpoints before failing the job
        // Optional: Configure the mode (exactly-once by default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // or AT_LEAST_ONCE

        // Optional: Specify the restart strategy in case of failure
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // Number of restart attempts
                Time.of(10, java.util.concurrent.TimeUnit.SECONDS) // Delay between attempts
        ));

        // Set up Kafka consumer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create a Kafka consumer that reads strings from a Kafka topic
        final var kafkaConsumer = new FlinkKafkaConsumer<>(
                "test", // Kafka topic
                new JsonNodeDeserializationSchema(), // Deserialization schema
                kafkaProperties // Kafka consumer properties
        );

        // Set the consumer to commit offsets on checkpoints
        kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true); // Commit Kafka offsets on checkpoints

        // Create a data stream by adding the Kafka source
        final var kafkaStream = env.addSource(kafkaConsumer);

        kafkaStream.print();

        env.execute("Kafka Source Example");
    }
}
