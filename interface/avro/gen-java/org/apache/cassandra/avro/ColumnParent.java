package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class ColumnParent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"ColumnParent\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"column_family\",\"type\":\"string\"},{\"name\":\"super_column\",\"type\":[\"bytes\",\"null\"]}]}");
  public org.apache.avro.util.Utf8 column_family;
  public java.nio.ByteBuffer super_column;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return column_family;
    case 1: return super_column;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: column_family = (org.apache.avro.util.Utf8)value$; break;
    case 1: super_column = (java.nio.ByteBuffer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
