package cn.stephen.example.kafka;

import cn.stephen.context.StreamEnvContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        StreamEnvContext.getKafkaNoWatermarksSource(env, args)
//                        .map((MapFunction<String, Tuple2<String, Integer>>) value -> {
//                            JSONObject courseObj = JSON.parseObject(value);
//                            String courseName = courseObj.getString("name");
//                            return new Tuple2<>(courseName, 1);
//                        })
//                .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(x -> x.f0)
//                .sum(1)
//                .print().setParallelism(1);

        DataStreamSource<String> fakeSource = StreamEnvContext.getFakeSource(env);
        Sink<String> kafkaSink = StreamEnvContext.getKafkaSink(args);
        fakeSource.sinkTo(kafkaSink);

        StreamEnvContext.jobExecute(env);
    }

}
