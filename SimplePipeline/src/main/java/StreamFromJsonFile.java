import datasource.JSONSourceFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamFromJsonFile {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<JsonNode> jsonStream = env.addSource(new JSONSourceFunction("/test.json"));

        jsonStream.print();

        env.execute("Custom JSON Source Example");
    }
}
