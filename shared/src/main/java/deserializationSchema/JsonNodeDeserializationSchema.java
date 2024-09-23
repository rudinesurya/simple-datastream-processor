package deserializationSchema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonNodeDeserializationSchema implements DeserializationSchema<JsonNode> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        // Deserialize the message into a JsonNode
        return objectMapper.readTree(new String(message));
    }

    @Override
    public boolean isEndOfStream(JsonNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}