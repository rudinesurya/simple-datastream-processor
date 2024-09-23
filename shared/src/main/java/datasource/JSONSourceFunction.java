package datasource;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class JSONSourceFunction<T> extends RichSourceFunction<T> {
    private final String filePath;
    private final DeserializationSchema<T> deserializationSchema;
    private volatile boolean isRunning = true;

    public JSONSourceFunction(String filePath, DeserializationSchema<T> deserializationSchema) {
        this.filePath = filePath;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while (isRunning) { // Check isRunning in the loop condition
                if ((line = reader.readLine()) == null) {
                    break; // Exit loop if the end of the stream is reached
                }
                try {
                    T element = deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
                    ctx.collect(element);
                } catch (Exception e) {
                    // Log deserialization errors, if needed
                    System.err.println("Failed to deserialize line: " + line);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            // Log any other errors that may occur during reading
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}