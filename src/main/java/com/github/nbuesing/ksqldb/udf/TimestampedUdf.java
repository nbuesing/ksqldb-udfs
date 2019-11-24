package com.github.nbuesing.ksqldb.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(
        name = "timestamped",
        description = "associate a timestamp with a value."
)
@SuppressWarnings("unused")
public class TimestampedUdf {

    private static final String VALUE = "VALUE";
    private static final String TIMESTAMPED = "TIMESTAMPED";
    private static final Schema SCHEMA = SchemaBuilder.struct().optional()
            .field(VALUE, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(TIMESTAMPED, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    @Udf(schema = "STRUCT<VALUE double, TIMESTAMPED bigint>",
            description = "associate a value with a timestamp")
    public Struct combine(
            @UdfParameter(value = "value", description = "value") final Double value,
            @UdfParameter(value = "timestamped", description = "timestamp for the value") final Long ts) {
        return new Struct(SCHEMA)
                .put(VALUE, value)
                .put(TIMESTAMPED, ts);
    }
}
