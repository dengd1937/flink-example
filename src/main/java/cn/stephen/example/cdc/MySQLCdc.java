package cn.stephen.example.cdc;

import cn.stephen.context.StreamEnvContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQLCdc {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8082);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        StreamEnvContext.getMySQLNoWatermarksCdcSource(env, args)
                        .print();

        StreamEnvContext.jobExecute(env);
    }

}
