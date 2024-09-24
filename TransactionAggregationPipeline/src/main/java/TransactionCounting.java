import datasource.JSONSourceFunction;
import deserializationSchema.TransactionDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.walkthrough.common.entity.Transaction;
import java.time.Duration;

@Slf4j
public class TransactionCounting {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var jsonSource = new JSONSourceFunction<>("test.json", new TransactionDeserializationSchema());
        final var transactions = env.addSource(jsonSource, TypeInformation.of(Transaction.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofMinutes(1)) // Adjust the bounded out of orderness
                                .withTimestampAssigner((transaction, timestamp) -> transaction.getTimestamp())
                );;

        transactions.print();

        DataStream<Tuple2<Long, Integer>> transactionCountStream = transactions
                .map(new MapFunction<Transaction, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Transaction transaction) throws Exception {
                        return new Tuple2<>(transaction.getAccountId(), 1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.minutes(2))
                .sum(1);

        transactionCountStream.print();

        env.execute("Transaction Counter Job");
    }
}
