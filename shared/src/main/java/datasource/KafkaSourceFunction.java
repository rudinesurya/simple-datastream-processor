package datasource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceFunction<T> extends RichSourceFunction<T> {
    private transient FlinkKafkaConsumer<T> kafkaConsumer;
    private final String topic;
    private final Properties kafkaProperties;
    private final DeserializationSchema<T> deserializationSchema;
    private volatile boolean isRunning = true;

    public KafkaSourceFunction(String topic, Properties kafkaProperties, DeserializationSchema<T> deserializationSchema) {
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                deserializationSchema,
                kafkaProperties
        );

        kafkaConsumer.setStartFromGroupOffsets();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        var kafkaStream = kafkaConsumer;
        while (isRunning) {
            kafkaStream.run(ctx);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
