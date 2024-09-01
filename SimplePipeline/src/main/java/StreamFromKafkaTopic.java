import datasource.KafkaDataSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamFromKafkaTopic {
    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topic = "test";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<JsonNode> jsonStream = env.addSource(new KafkaDataSource(brokers, topic, 5L));

        jsonStream.print();

        env.execute("Kafka Source Example");
    }
}
