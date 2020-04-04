package flink.learn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TextTokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    private static final long serialVersionUID = 2L;

    @Override
    public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if(str == null || str.isEmpty()){
            return;
        }

        String[] words = str.split("\\s+");

        for (String word : words ){
            if(word.contains(".") || word.contains(",") || word.contains("’s")){
                word = word.replaceAll(".|,|(’s)","");
            }
            word = word.trim();
            if(word.isEmpty()){
                continue;
            }
            if(!word.equals("I")){
                word = word.toLowerCase();
            }
            collector.collect(new Tuple2<>(word,1));
        }
    }

}
