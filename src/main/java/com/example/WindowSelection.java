package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.Event;
import model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import repository.KeyValueRepository;
import repository.RocksDBRepository;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public final class WindowSelection {

    public static void main(String[] args) {
        final StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<String> text = executionEnvironment
                .socketTextStream("localhost", 9000, '\n', 6);

        KeyValueRepository<String, Object> metricId = new RocksDBRepository();
        KeyValueRepository<String, Object> idTimeSeries = new RocksDBRepository();
        AtomicInteger counter = new AtomicInteger();

        final DataStream <WordWithCount> countWindowWordCount =
                text.flatMap((FlatMapFunction<String, WordWithCount>)
                        (textStream, wordCountKeyPair) -> {
                            ObjectMapper objectMapper = new ObjectMapper();
                            Event event = objectMapper.readValue(textStream, Event.class);
                            String word = event.toString();
                            if(Objects.isNull(metricId.find(word))){
                                metricId.save(word, counter.getAndIncrement());
                            }
                            wordCountKeyPair.collect(new WordWithCount(word, 1L));
                        }, TypeInformation.of(WordWithCount.class))
                        .keyBy((KeySelector<WordWithCount, String>) wordWithCount -> wordWithCount.word,
                                TypeInformation.of(String.class))
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                        .reduce((ReduceFunction<WordWithCount>)
                                (a, b) ->{
                                    /**
                                     * Find ID based on key.
                                     * Based on current timeStamp store the Key value pair as TIMESTAMP@ID -> [0, 1, 2, 3]
                                     * We can take a bucket size here of 1 minute.
                                     */
                            return new WordWithCount(a.word, a.count + b.count); });

        // print the results with a single thread, rather than in parallel
        countWindowWordCount.print();
        executionEnvironment.execute("Flink Tumbling window Example");
    }

}
