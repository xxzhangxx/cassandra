package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class Column extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Column\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"clock\",\"type\":{\"type\":\"record\",\"name\":\"Clock\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"context\",\"type\":[\"bytes\",\"null\"]}]}},{\"name\":\"ttl\",\"type\":[\"int\",\"null\"]}]}");
  public java.nio.ByteBuffer name;
  public java.nio.ByteBuffer value;
  public org.apache.cassandra.avro.Clock clock;
  public java.lang.Integer ttl;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return value;
    case 2: return clock;
    case 3: return ttl;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.nio.ByteBuffer)value$; break;
    case 1: value = (java.nio.ByteBuffer)value$; break;
    case 2: clock = (org.apache.cassandra.avro.Clock)value$; break;
    case 3: ttl = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}