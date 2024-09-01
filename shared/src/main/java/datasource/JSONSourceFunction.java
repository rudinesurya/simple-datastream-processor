package datasource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;

public class JSONSourceFunction implements SourceFunction<JsonNode> {
    private volatile boolean isRunning = true;
    private ObjectMapper objectMapper = new ObjectMapper();
    private final String resourcePath;

    public JSONSourceFunction(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    @Override
    public void run(SourceContext<JsonNode> ctx) throws Exception {
        // Load the file from resources using getClass().getResourceAsStream()
        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String jsonString;
            while (isRunning && (jsonString = reader.readLine()) != null) {
                // Parse each line as a JSON object
                JsonNode jsonNode = objectMapper.readTree(jsonString);
                long eventTime = jsonNode.get("timestamp").asLong();

                ctx.collectWithTimestamp(jsonNode, eventTime);
                ctx.emitWatermark(new Watermark(eventTime - 1));
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}