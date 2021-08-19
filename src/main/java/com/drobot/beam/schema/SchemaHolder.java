package com.drobot.beam.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;

import java.util.Arrays;

@SuppressWarnings({"unused", "SameParameterValue"})
public class SchemaHolder {

    private static final org.apache.avro.Schema AVRO_RECORD_SCHEMA;
    private static final org.apache.beam.sdk.schemas.Schema BEAM_RECORD_SCHEMA;
    private static final TableSchema BQ_TABLE_SCHEMA;

    private SchemaHolder() {
    }

    static {
        AVRO_RECORD_SCHEMA = createAvroRecordSchema();
        BEAM_RECORD_SCHEMA = createBeamRecordSchema();
        BQ_TABLE_SCHEMA = createBqTableSchema();
    }

    public static org.apache.avro.Schema getAvroRecordSchema() {
        return AVRO_RECORD_SCHEMA;
    }

    public static org.apache.beam.sdk.schemas.Schema getBeamRecordSchema() {
        return BEAM_RECORD_SCHEMA;
    }

    public static TableSchema getBqTableSchema() {
        return BQ_TABLE_SCHEMA;
    }

    private static org.apache.avro.Schema createAvroRecordSchema() {
        return new org.apache.avro.Schema.Parser().parse("{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"topLevelRecord\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"id\",\n" +
                "    \"type\" : [ \"long\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"date_time\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"site_name\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"posa_continent\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"user_location_country\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"user_location_region\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"user_location_city\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"orig_destination_distance\",\n" +
                "    \"type\" : [ \"double\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"user_id\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"is_mobile\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"is_package\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"channel\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_ci\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_co\",\n" +
                "    \"type\" : [ \"string\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_adults_cnt\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_children_cnt\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_rm_cnt\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_destination_id\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"srch_destination_type_id\",\n" +
                "    \"type\" : [ \"int\", \"null\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"hotel_id\",\n" +
                "    \"type\" : [ \"long\", \"null\" ]\n" +
                "  } ]\n" +
                "}");
    }

    private static org.apache.beam.sdk.schemas.Schema createBeamRecordSchema() {
        return AvroUtils.toBeamSchema(AVRO_RECORD_SCHEMA);
    }

    private static TableSchema createBqTableSchema() {
        return new TableSchema()
                .setFields(
                        Arrays.asList(
                                longNullableField("id"),
                                stringNullableField("date_time"),
                                integerNullableField("site_name"),
                                integerNullableField("posa_continent"),
                                integerNullableField("user_location_country"),
                                integerNullableField("user_location_region"),
                                integerNullableField("user_location_city"),
                                doubleNullableField("orig_destination_distance"),
                                integerNullableField("user_id"),
                                integerNullableField("is_mobile"),
                                integerNullableField("is_package"),
                                integerNullableField("channel"),
                                stringNullableField("srch_ci"),
                                stringNullableField("srch_co"),
                                integerNullableField("srch_adults_cnt"),
                                integerNullableField("srch_children_cnt"),
                                integerNullableField("srch_rm_cnt"),
                                integerNullableField("srch_destination_id"),
                                integerNullableField("srch_destination_type_id"),
                                longNullableField("hotel_id")
                        )
                );
    }

    private static TableFieldSchema integerNullableField(String name) {
        return new TableFieldSchema().setName(name).setType("INTEGER");
    }

    private static TableFieldSchema stringNullableField(String name) {
        return new TableFieldSchema().setName(name).setType("STRING");
    }

    private static TableFieldSchema longNullableField(String name) {
        return new TableFieldSchema().setName(name).setType("INT64");
    }

    private static TableFieldSchema doubleNullableField(String name) {
        return new TableFieldSchema().setName(name).setType("DOUBLE");
    }
}