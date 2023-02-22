package cn.stephen.example.cdc;


import com.alibaba.fastjson.JSON;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.time.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.text.ParseException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private final String DATE_PATTERN = "yyyy-MM-dd";
    private final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private final String DATETIME_INSTANT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final JsonDebeziumDeserializationSchema.DeserializationRuntimeConverter runtimeConverter;

    public JsonDebeziumDeserializationSchema(int zoneOffset) {
        //实现一个用于转换时间的Converter
        this.runtimeConverter = (dbzObj,schema) -> {
            if(schema.name() != null){
                switch (schema.name()) {
                    case Timestamp.SCHEMA_NAME:
                        return TimestampData.fromEpochMillis((Long) dbzObj)
                                .toLocalDateTime()
                                .atOffset(ZoneOffset.ofHours(zoneOffset))
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case ZonedTimestamp.SCHEMA_NAME:
                        FastDateFormat utcFormat = FastDateFormat.getInstance(DATETIME_INSTANT_PATTERN, UTC);
                        java.util.Date date = utcFormat.parse((String) dbzObj);
                        return FastDateFormat.getInstance(DATETIME_PATTERN).format(date);
                    case MicroTimestamp.SCHEMA_NAME:
                        long micro = (long) dbzObj;
                        return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000))
                                .toLocalDateTime()
                                .atOffset(ZoneOffset.ofHours(zoneOffset))
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case NanoTimestamp.SCHEMA_NAME:
                        long nano = (long) dbzObj;
                        return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000))
                                .toLocalDateTime()
                                .atOffset(ZoneOffset.ofHours(zoneOffset))
                                .format(DateTimeFormatter.ofPattern(DATETIME_PATTERN));
                    case Date.SCHEMA_NAME:
                        return TemporalConversions.toLocalDate(dbzObj).format(DateTimeFormatter.ofPattern(DATE_PATTERN));
                }
            }
            return dbzObj;
        };
    }

    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object dbzObj, Schema schema) throws ParseException;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        HashMap<String, Object> changes = new HashMap<>();

        Struct key = (Struct) record.key();
        changes.put("pk", getRowMap(key));

        Envelope.Operation operation = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            // insert
            changes.put("type", "insert");
            changes.put("data", getRowMap(value.getStruct(Envelope.FieldName.AFTER)));
        } else if (operation == Envelope.Operation.UPDATE) {
            // update
            changes.put("type", "update");
            changes.put("data", getRowMap(value.getStruct(Envelope.FieldName.AFTER)));
        } else if (operation == Envelope.Operation.DELETE) {
            // delete
            changes.put("type", "delete");
            changes.put("data", getRowMap(value.getStruct(Envelope.FieldName.BEFORE)));
        }

        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String db = source.getString("db");
        String table = source.getString("table");
        changes.put("table", source.getString("connector").equals("sqlserver")
                ? StringUtils.join(db, ".", source.getString("schema"), ".", table)
                : StringUtils.join(db, ".", table));
        changes.put("ts", source.getInt64("ts_ms"));

        out.collect(JSON.toJSONString(changes));
//        out.collect(record.toString());
    }

    private Map<String, Object> getRowMap(Struct struct) {
        return struct.schema().fields().stream()
                .collect(Collectors.toMap(Field::name, f-> {
                    try {
                        return runtimeConverter.convert(struct.get(f),f.schema());
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    private Map<String, String> getRowSchema(Schema schema) {
        return schema.fields().stream().collect(Collectors.toMap(Field::name, f -> f.schema().type().getName()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
