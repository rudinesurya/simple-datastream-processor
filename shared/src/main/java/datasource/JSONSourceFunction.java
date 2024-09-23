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
            while (isRunning && (line = reader.readLine()) != null) {
                T element = deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8));
                ctx.collect(element);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}