package flink.learn.bounded.data;

import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;

import java.io.Serializable;
import java.util.Iterator;

public class WordCountSource extends FromIteratorFunction<String> implements Serializable {

    private static final long serialVersionUID = 0L;

    public WordCountSource() {
        super(new WordIterator());
    }

    private static class WordIterator implements Iterator<String>,Serializable{

        private static final long serialVersionUID = 4L;

        private int index = 0;
        private int length = -1;

        private WordIterator(){
            length = StaticWordData.STEVE_JOBS_WORD.length;
        }

        @Override
        public boolean hasNext() {
            return (index < length);
        }

        @Override
        public String next() {
            return StaticWordData.STEVE_JOBS_WORD[index ++];
        }
    }

}
