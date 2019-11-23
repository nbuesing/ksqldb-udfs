package com.github.nbuesing.ksqldb.udf;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "accelerate", description = "acceleration based on velocity (value and time) structure.")
public final class AccelerationUdf {

    private static final String PV = "PV";
    private static final String CV = "CV";
    private static final String PT = "PT";
    private static final String CT = "CT";

    private static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
            .field(PV, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(CV, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(PT, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CT, Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    private AccelerationUdf() {
    }

    @UdafFactory(
            paramSchema = "STRUCT<VELOCITY double, TS bigint>",
            aggregateSchema = "STRUCT<PV double, CV double, PT bigint, CT bigint>",
            description = "acceleration instance with Double as the mapped output."
    )
    public static TableUdaf<Struct, Struct, Double> accelDouble() {
        return new TableUdaf<Struct, Struct, Double>() {
            @Override
            public Struct initialize() {
                return new Struct(STRUCT_DOUBLE)
                        .put(PV, null)
                        .put(CV, null)
                        .put(PT, null)
                        .put(CT, null);
            }

            @Override
            public Struct aggregate(final Struct val, final Struct aggregate) {

                System.out.println("val : " + val);
                System.out.println("agg : " + aggregate);

                if (val == null) {
                    return aggregate;
                }

                Struct s = new Struct(STRUCT_DOUBLE)
                        .put(PV, aggregate.getFloat64(CV))
                        .put(PT, aggregate.getInt64(CT))
                        .put(CV, val.getFloat64("VELOCITY"))
                        .put(CT, val.getInt64("TS"));

                System.out.println(s);

                return s;
            }

            @Override
            public Struct merge(final Struct aggOne, final Struct aggTwo) {
//                return aggTwo;
                return new Struct(STRUCT_DOUBLE)
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
