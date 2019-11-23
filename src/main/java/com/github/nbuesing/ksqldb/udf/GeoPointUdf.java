package com.github.nbuesing.ksqldb.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * KSQL is case-insensitive, ElasticSearch is case-sensitive.
 *
 * A structure with fields that are LAT and LON will not be processed in ElasticSearch as a geo_point.
 *
 * There are a variety of ways to get elastic search to handle data as a geo_point, see their documentation
 * here:
 *
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html
 *
 * This UDF will use the string representation as there is no additional text needed to worry about being
 * mis-interpreted.
 *
 */
@UdfDescription(name = "geo_point", description = ".")
@SuppressWarnings("unused")
public class GeoPointUdf {

    private static final String LATITUDE = "LAT";
    private static final String LONGITUDE = "LON";

    private static final Schema SCHEMA = SchemaBuilder.struct().optional()
            .field(LATITUDE, Schema.FLOAT64_SCHEMA)
            .field(LONGITUDE, Schema.FLOAT64_SCHEMA)
            .build();

    @Udf(description = "convert structured location into a string so the location can be indexed by elastic-search.")
    public String combine(@UdfParameter(schema = "STRUCT<LAT double, LON double>", value = "location", description = "location (lat,lon)") final Struct location) {
        return location.getFloat64(LATITUDE) + "," + location.getFloat64(LONGITUDE);
    }

}
