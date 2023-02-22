package cn.stephen.context;

public class StreamSourceConfig {

    /*
     * Kafka Source
     */
    public static final String KAFKA_REQUIRED_BOOTSTRAP_SERVER = "kafka_source_server";
    public static final String KAFKA_REQUIRED_TOPIC = "kafka_source_topic";
    public static final String KAFKA_REQUIRED_GROUP = "kafka_source_group";
    public static final String KAFKA_OPTIONAL_OFFSET = "kafka_source_offset";
    public static final String KAFKA_DEFAULT_SOURCE_NAME = "Kafka Source";


    /*
     * CDC Source
     */
    public static final String CDC_REQUIRED_HOST = "cdc_source_host";
    public static final String CDC_OPTIONAL_PORT = "cdc_source_port";
    public static final String CDC_REQUIRED_USER = "cdc_source_user";
    public static final String CDC_REQUIRED_PASSWORD = "cdc_source_password";
    public static final String CDC_REQUIRED_DATABASES = "cdc_source_databases";
    public static final String CDC_REQUIRED_TABLES = "cdc_source_tables";


}
