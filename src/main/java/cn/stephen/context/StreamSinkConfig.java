package cn.stephen.context;

public class StreamSinkConfig {

    /*
     * Kafka Sink
     */
    public static final String KAFKA_REQUIRED_BOOTSTRAP_SERVER = "kafka_sink_server";
    public static final String KAFKA_REQUIRED_TOPIC = "kafka_sink_topic";

    /*
     * File Sink
     */
    public static final String FILE_REQUIRED_PATH = "file_sink_path";
    public static final String FILE_OPTIONAL_ROLLOVER_SECOND_INTERVAL = "file_sink_rollover_second_interval";
    public static final String FILE_OPTIONAL_INACTIVE_SECOND_INTERVAL = "file_sink_inactive_second_interval";
    public static final String FILE_OPTIONAL_MAX_PART_SIZE = "file_sink_max_part_size";
    public static final String FILE_OPTIONAL_BUCKET_CHECK_MILLI_INTERVAL = "file_sink_bucket_check_milli_interval";
    public static final String FILE_OPTIONAL_OUT_FILE_PREFIX = "file_sink_out_file_prefix";
    public static final String FILE_OPTIONAL_OUT_FILE_SUFFIX = "file_sink_out_file_suffix";
    public static final String FILE_OPTIONAL_BUCKET_FORMAT = "file_sink_bucket_format";


}
