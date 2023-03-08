package cn.stephen.example.file;

import cn.stephen.context.StreamEnvContext;
import cn.stephen.example.datagen.Course;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

public class FileSinkExample {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(1000);

        DataStreamSource<String> fakeSource = StreamEnvContext.getIntervalFakeSource(env, 1000L);
//        Sink<String> fileSink = StreamEnvContext.getRowFormatFileSink(args);
        //        fakeSource.sinkTo(fileSink);

//        Sink<Course> fileSink = StreamEnvContext.getParquetFormatFileSink(args, Course.class);

        // TODO bean生成schema
        String schema = "struct<_col0:int,_col1:string,_col2:int,_col3:string,_col4:string,_col5:double,_col6:string,_col7:int,_col8:bigint>";
        Sink<Course> fileSink = StreamEnvContext.getOrcFormatFileSink(args, new CourseVectorizer(schema));

        fakeSource.map(new MapFunction<String, Course>() {
            @Override
            public Course map(String value) throws Exception {
                return JSON.parseObject(value, Course.class);
            }
        }).sinkTo(fileSink);


        StreamEnvContext.jobExecute(env);
    }

}
