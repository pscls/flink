package org.apache.flink.connector.rabbitmq2.source.avro;

import org.apache.flink.avro.shaded.org.apache.avro.Schema;
import org.apache.flink.avro.shaded.org.apache.avro.SchemaBuilder;
import org.apache.flink.avro.shaded.org.apache.avro.specific.SpecificRecord;

public class User implements SpecificRecord {
    private String name;
    private int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
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
                return this.name;
            case 1:
                return this.age;
            default:
                return null;
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                name = (String) value;
            case 1:
                age = (int) value;
            default:
        }
    }
}
