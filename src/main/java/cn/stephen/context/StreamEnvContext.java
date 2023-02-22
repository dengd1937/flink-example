package cn.stephen.context;

import cn.stephen.example.cdc.JsonDebeziumDeserializationSchema;
import cn.stephen.example.datagen.FakeSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;

public class StreamEnvContext {

    public static void jobExecute(StreamExecutionEnvironment env) throws Exception {
        env.execute(Thread.currentThread().getStackTrace()[2].getClassName());
    }

    /* ===================================================Source==================================================== */
    public static DataStreamSource<String> getFakeSource (
            StreamExecutionEnvironment env) {
        return env.addSource(new FakeSource());
    }

    public static DataStreamSource<String> getKafkaNoWatermarksSource(
            StreamExecutionEnvironment env, String[] args) {
        return getKafkaSource(env, args, null);
    }

    public static DataStreamSource<String> getKafkaBoundedOutWatermarksSource(
            StreamExecutionEnvironment env, String[] args, Duration duration) {
        return getKafkaSource(env, args, WatermarkStrategy.forBoundedOutOfOrderness(duration));
    }

    public static DataStreamSource<String> getMySQLNoWatermarksCdcSource(
            StreamExecutionEnvironment env, String[] args) {
        return getMySQLCdcSource(env, args, WatermarkStrategy.noWatermarks());
    }

    private static DataStreamSource<String> getMySQLCdcSource(
            StreamExecutionEnvironment env, String[] args,
            WatermarkStrategy<String> watermarkStrategy) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String host = fromArgs.getRequired(StreamSourceConfig.CDC_REQUIRED_HOST);
        int port = fromArgs.getInt(StreamSourceConfig.CDC_OPTIONAL_PORT, 3306);
        String user = fromArgs.getRequired(StreamSourceConfig.CDC_REQUIRED_USER);
        String password = fromArgs.getRequired(StreamSourceConfig.CDC_REQUIRED_PASSWORD);
        String[] databases = fromArgs.getRequired(StreamSourceConfig.CDC_REQUIRED_DATABASES).split(",");
        String[] tables = fromArgs.get(StreamSourceConfig.CDC_REQUIRED_TABLES, ".*").split(",");

        Properties jdbcProperties = new Properties();
        jdbcProperties.put("connectionTimeZone", "Asia/Shanghai");
        jdbcProperties.put("useSSL", "false");
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname(host)
                .port(port)
                .username(user)
                .password(password)
                .databaseList(databases)
                .tableList(tables)
                .jdbcProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema(8))
                .build();
        return env.fromSource(source, watermarkStrategy, "MySQL CDC Source");
    }

    private static DataStreamSource<String> getKafkaSource(
            StreamExecutionEnvironment env, String[] args,
            WatermarkStrategy<String> watermarkStrategy) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String server = fromArgs.getRequired(StreamSourceConfig.KAFKA_REQUIRED_BOOTSTRAP_SERVER);
        String topic = fromArgs.getRequired(StreamSourceConfig.KAFKA_REQUIRED_TOPIC);
        String group = fromArgs.getRequired(StreamSourceConfig.KAFKA_REQUIRED_GROUP);
        OffsetsInitializer offsetInit = fromArgs.has(StreamSourceConfig.KAFKA_OPTIONAL_OFFSET)
                ? OffsetsInitializer.timestamp(fromArgs.getLong(StreamSourceConfig.KAFKA_OPTIONAL_OFFSET))
                : OffsetsInitializer.latest();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(server)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(offsetInit)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source,
                watermarkStrategy == null ? WatermarkStrategy.noWatermarks() : watermarkStrategy,
                StreamSourceConfig.KAFKA_DEFAULT_SOURCE_NAME);
    }

    /* =====================================================Sink==================================================== */
    public static Sink<String> getKafkaSink(String[] args) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String server = fromArgs.getRequired(StreamSinkConfig.KAFKA_REQUIRED_BOOTSTRAP_SERVER);
        String topic = fromArgs.getRequired(StreamSinkConfig.KAFKA_REQUIRED_TOPIC);

        Properties sinkPro = new Properties();
        sinkPro.put("transaction.timeout.ms", 15*60*1000);

        return KafkaSink.<String>builder()
                .setBootstrapServers(server)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(sinkPro)
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }




}
