package flink.learn.bounded;

import flink.learn.bounded.data.WordCountSink;
import flink.learn.bounded.data.WordCountSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountJob  {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.addSource(new WordCountSource())
                .name("SteveJobs-Word");

        DataStream<Tuple2<String,Integer>> proc = source.flatMap(new TextTokenizer())
                .keyBy(0)
                .sum(1)
                .name("Process-Word-Count");

        proc.addSink(new WordCountSink())
                .name("Word-Count-Sink");

        env.execute("Word Count");
    }

}
