/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()))
                .keyBy((TaxiFare t) -> t.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                // reduce了就不再是keyedStream了
                .reduce(new ReduceFunction<TaxiFare>() {
                    @Override
                    public TaxiFare reduce(TaxiFare value1, TaxiFare value2) throws Exception {
                        value1.tip = value1.tip + value2.tip;
                        return value1;
                    }
                });
        // 还有一种写法,可以按照window.endTime进行keyBy这样的状态会小很多
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
                // 不是keyedStream就可以windowAll去处理窗口下所有的key了
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessAllWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
                        System.out.println(elements);
                        TaxiFare max = elements.iterator().next();
                        float total = 0;
                        for (TaxiFare taxiFare : elements) {
                            if (taxiFare.tip > max.tip) {
                                max = taxiFare;
                            }
                            total += taxiFare.tip;
                        }
                        out.collect(new Tuple3<>(context.window().getEnd(), max.driverId, total));
                    }
                });

        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

}
