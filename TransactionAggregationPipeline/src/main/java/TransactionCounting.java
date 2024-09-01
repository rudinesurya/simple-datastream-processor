import datasource.KafkaDataSource;
import mapFunction.MapToTransaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.walkthrough.common.entity.Transaction;


public class TransactionCounting {
    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String topic = "test";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new KafkaDataSource(brokers, topic, 0L))
                .map(new MapToTransaction())
                .name("transactions");

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
