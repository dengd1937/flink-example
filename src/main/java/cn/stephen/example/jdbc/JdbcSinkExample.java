package cn.stephen.example.jdbc;

import cn.stephen.context.StreamEnvContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSinkExample {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        DataStreamSource<String> fakeSource = StreamEnvContext.getFakeSource(env);


        StreamEnvContext.jobExecute(env);
    }

}
