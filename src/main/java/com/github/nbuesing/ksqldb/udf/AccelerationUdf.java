package com.github.nbuesing.ksqldb.udf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
        name = "acceleration",
        description = "acceleration based on velocity (value and time)."
)
public final class AccelerationUdf {

    private static final String PV = "PV";
    private static final String PT = "PT";
    private static final String CV = "CV";
    private static final String CT = "CT";

    private static final String VALUE = "VALUE";
    private static final String TIMESTAMPED = "TIMESTAMPED";

    // make sure this matches UdafFactories 'aggregateSchema' exactly or you will
    // struct mismatch exception w/out indication of what struct was wrong.
    private static final Schema STRUCT = SchemaBuilder.struct().optional()
            .field(PV, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(PT, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CV, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(CT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private AccelerationUdf() {
    }

    @UdafFactory(
            paramSchema = "STRUCT<VALUE double, TIMESTAMPED bigint>",
            aggregateSchema = "STRUCT<PV double, PT bigint, CV double, CT bigint>",
            description = "acceleration instance with Double as the mapped output."
    )
    public static TableUdaf<Struct, Struct, Double> accelDouble() {
        return new TableUdaf<Struct, Struct, Double>() {
            @Override
            public Struct initialize() {
                return new Struct(STRUCT)
                        .put(PV, null)
                        .put(PT, null)
                        .put(CV, null)
                        .put(CT, null);
            }

            @Override
            public Struct aggregate(final Struct val, final Struct aggregate) {
                return (val != null) ? new Struct(STRUCT)
                        .put(PV, aggregate.getFloat64(CV))
                        .put(PT, aggregate.getInt64(CT))
                        .put(CV, val.getFloat64(VALUE))
                        .put(CT, val.getInt64(TIMESTAMPED)) : aggregate;
            }

            @Override
            public Struct merge(final Struct aggOne, final Struct aggTwo) {
                //ignoring merge scenarios.
                return new Struct(STRUCT)
                        .put(PV, null)
                        .put(CV, null);
            }

            @Override
            public Double map(final Struct agg) {

                Double previous = agg.getFloat64(PV);
                Double current = agg.getFloat64(CV);
                Long previousT = agg.getInt64(PT);
                Long currentT = agg.getInt64(CT);

                if (previous == null || current == null) {
                    return 0.0;
                }

                return (current - previous) / ((double) (currentT - previousT) / 1000.0);
            }

            @Override
            public Struct undo(final Struct val, final Struct agg) {
                return agg;
            }
        };
    }
}
