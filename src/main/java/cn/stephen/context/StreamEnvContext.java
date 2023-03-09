package cn.stephen.context;

import cn.stephen.example.cdc.JsonDebeziumDeserializationSchema;
import cn.stephen.example.datagen.FakeSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.time.Duration;
import java.util.Properties;

public class StreamEnvContext {

    public static void jobExecute(StreamExecutionEnvironment env) throws Exception {
        env.execute(Thread.currentThread().getStackTrace()[2].getClassName());
    }

    /* ===================================================Source==================================================== */
    public static DataStreamSource<String> getFakeSource(
            StreamExecutionEnvironment env) {
        return env.addSource(new FakeSource());
    }

    public static DataStreamSource<String> getIntervalFakeSource(
            StreamExecutionEnvironment env, Long emitInterval) {
        return env.addSource(new FakeSource(emitInterval));
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
        sinkPro.put("transaction.timeout.ms", 15 * 60 * 1000);

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

    public static Sink<String> getRowFormatFileSink(String[] args) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String filePath = fromArgs.getRequired(StreamSinkConfig.FILE_REQUIRED_PATH);
        long rolloverInterval = Long.parseLong(
                fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_ROLLOVER_SECOND_INTERVAL, "60"));
        long inactiveInterval = Long.parseLong(
                fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_INACTIVE_SECOND_INTERVAL, "10"));
        long maxPartSize = Long.parseLong(
                fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_MAX_PART_SIZE, "134217728"));
        long bucketCheckInterval = Long.parseLong(
                fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_BUCKET_CHECK_MILLI_INTERVAL, "60000")
        );
        String outfilePrefix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_PREFIX, "part");
        String outfileSuffix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_SUFFIX, ".txt");
        String bucketFormat = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_BUCKET_FORMAT, "yyyyMMddHH");


        return FileSink.forRowFormat(new Path(filePath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(rolloverInterval))
                                .withInactivityInterval(Duration.ofSeconds(inactiveInterval))
                                .withMaxPartSize(new MemorySize(maxPartSize))
                                .build()
                )
                .withBucketCheckInterval(bucketCheckInterval)
                .withOutputFileConfig(new OutputFileConfig(outfilePrefix, outfileSuffix))
                .withBucketAssigner(new DateTimeBucketAssigner<>(bucketFormat))
                .build();
    }

    public static <T> Sink<T> getParquetFormatFileSink(String[] args, Class<T> obj) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String filePath = fromArgs.getRequired(StreamSinkConfig.FILE_REQUIRED_PATH);
        String outfilePrefix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_PREFIX, "part");
        String outfileSuffix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_SUFFIX, ".parquet");
        String bucketFormat = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_BUCKET_FORMAT, "yyyyMMddHH");


        return FileSink.forBulkFormat(new Path(filePath), AvroParquetWriters.forReflectRecord(obj))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<>(bucketFormat))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix(outfilePrefix)
                                .withPartSuffix(outfileSuffix)
                                .build()
                )
                .build();
    }

    public static <T> Sink<T> getOrcFormatFileSink(String[] args, Vectorizer<T> vectorizer) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String filePath = fromArgs.getRequired(StreamSinkConfig.FILE_REQUIRED_PATH);
        String outfilePrefix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_PREFIX, "part");
        String outfileSuffix = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_OUT_FILE_SUFFIX, ".orc");
        String bucketFormat = fromArgs.get(StreamSinkConfig.FILE_OPTIONAL_BUCKET_FORMAT, "yyyyMMddHH");

        return FileSink.forBulkFormat(new Path(filePath), new OrcBulkWriterFactory<>(vectorizer))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<>(bucketFormat))
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix(outfilePrefix)
                                .withPartSuffix(outfileSuffix)
                                .build()
                )
                .build();
    }

    public static <T> SinkFunction<T> getJdbcSink(String[] args, String sql, JdbcStatementBuilder<T> statementBuilder) {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String url = fromArgs.getRequired(StreamSinkConfig.JDBC_REQUIRED_URL);
        String driverName = fromArgs.getRequired(StreamSinkConfig.JDBC_REQUIRED_DRIVER_NAME);
        String username = fromArgs.getRequired(StreamSinkConfig.JDBC_REQUIRED_USERNAME);
        String password = fromArgs.getRequired(StreamSinkConfig.JDBC_REQUIRED_PASSWORD);
        int batchIntervalMs = Integer.parseInt(
                fromArgs.get(StreamSinkConfig.JDBC_OPTIONAL_BATCH_INTERVAL_MS, "100")
        );
        int batchSize= Integer.parseInt(
                fromArgs.get(StreamSinkConfig.JDBC_OPTIONAL_BATCH_SIZE, "1000")
        );
        int maxRetries= Integer.parseInt(
                fromArgs.get(StreamSinkConfig.JDBC_OPTIONAL_MAX_RETRIES, "3")
        );


        return JdbcSink.sink(
                sql,
                statementBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(batchIntervalMs)
                        .withBatchSize(batchSize)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driverName)
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }

}
