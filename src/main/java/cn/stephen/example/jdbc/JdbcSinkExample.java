package cn.stephen.example.jdbc;

import cn.stephen.context.StreamEnvContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class JdbcSinkExample {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));

        String sql = "insert into mydb.users(id, name, age) values(?, ?, ?) on duplicate key update name=?,age=?";
        SinkFunction<String> jdbcSink = StreamEnvContext.getMySQLExactlyOnceSink(args, sql, (preparedStatement, s) -> {
            String[] fields = s.split(",");
            preparedStatement.setLong(1, Long.parseLong(fields[0]));
            preparedStatement.setString(2, fields[1]);
            preparedStatement.setInt(3, Integer.parseInt(fields[2]));
            preparedStatement.setString(4, fields[1]);
            preparedStatement.setInt(5, Integer.parseInt(fields[2]));
        });

        env.socketTextStream("hadoop001", 9527)
                .addSink(jdbcSink);


        StreamEnvContext.jobExecute(env);
    }

}
