package org.apache.flink.connector.rabbitmq2.source.avro;

import org.apache.flink.avro.shaded.org.apache.avro.Schema;
import org.apache.flink.avro.shaded.org.apache.avro.SchemaBuilder;
import org.apache.flink.avro.shaded.org.apache.avro.specific.SpecificRecord;

public class User implements SpecificRecord {
    public String timestamps;

    public User() {
    }

    @Override
    public Schema getSchema() {
        return SchemaBuilder.record("User")
                .namespace("example.avro")
                .fields()
                    .requiredString("name")
                    .requiredInt("age")
                .endRecord();
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0:
                return this.timestamps;
            default:
                return null;
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                timestamps = (String) value;
            default:
        }
    }
}
