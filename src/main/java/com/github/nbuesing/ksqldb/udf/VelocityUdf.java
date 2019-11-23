package com.github.nbuesing.ksqldb.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(name = "velocity", description = ".")
@SuppressWarnings("unused")
public class VelocityUdf {

    private static final String VELOCITY = "VELOCITY";
    private static final String TIMESTAMP = "TS";

    private static final Schema SCHEMA = SchemaBuilder.struct().optional()
            .field(VELOCITY, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    @Udf(schema = "STRUCT<VELOCITY double, TS bigint>", description = "capture velocity in addition to the timestamp of when velocity was obtained.")
    public Struct combine(@UdfParameter(value = "value", description = "velocity value") final Double velocity, @UdfParameter(value = "timestamp", description = "timestamp of when velocity was obtained") final Long ts) {
        return new Struct(SCHEMA)
                .put(VELOCITY, velocity)
                .put(TIMESTAMP, ts);
    }

}
