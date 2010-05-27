package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class Deletion extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Deletion\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"clock\",\"type\":{\"type\":\"record\",\"name\":\"Clock\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"context\",\"type\":[\"bytes\",\"null\"]}]}},{\"name\":\"super_column\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"predicate\",\"type\":[{\"type\":\"record\",\"name\":\"SlicePredicate\",\"fields\":[{\"name\":\"column_names\",\"type\":[{\"type\":\"array\",\"items\":\"bytes\"},\"null\"]},{\"name\":\"slice_range\",\"type\":[{\"type\":\"record\",\"name\":\"SliceRange\",\"fields\":[{\"name\":\"start\",\"type\":\"bytes\"},{\"name\":\"finish\",\"type\":\"bytes\"},{\"name\":\"reversed\",\"type\":\"boolean\"},{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"bitmasks\",\"type\":{\"type\":\"array\",\"items\":\"bytes\"}}]},\"null\"]}]},\"null\"]}]}");
  public org.apache.cassandra.avro.Clock clock;
  public java.nio.ByteBuffer super_column;
  public org.apache.cassandra.avro.SlicePredicate predicate;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return clock;
    case 1: return super_column;
    case 2: return predicate;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: clock = (org.apache.cassandra.avro.Clock)value$; break;
    case 1: super_column = (java.nio.ByteBuffer)value$; break;
    case 2: predicate = (org.apache.cassandra.avro.SlicePredicate)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
