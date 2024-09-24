package mapFunction;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.walkthrough.common.entity.Transaction;

@Slf4j
public class MapToTransaction implements MapFunction<JsonNode, Transaction> {
    private static ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper for JSON parsing

    @Override
    public Transaction map(JsonNode jsonNode) throws Exception {
        try {
            // Convert JsonNode to Transaction using ObjectMapper
            return objectMapper.treeToValue(jsonNode, Transaction.class);
        } catch (Exception e) {
            // Handle the exception as needed
            throw new RuntimeException("Failed to convert JsonNode to Transaction", e);
        }
    }
}
