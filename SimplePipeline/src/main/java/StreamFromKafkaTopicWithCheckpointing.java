import datasource.KafkaSourceFunction;
import deserializationSchema.JsonNodeDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamFromKafkaTopicWithCheckpointing {
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


        final var jsonSource = new KafkaSourceFunction<>("test", "localhost:9092", "my-group", new JsonNodeDeserializationSchema());
        final var jsonStream = env.addSource(jsonSource, TypeInformation.of(JsonNode.class));
        jsonStream.print();

        env.execute("Kafka Source Example");
    }
}
