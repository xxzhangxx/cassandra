package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class SlicePredicate extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"SlicePredicate\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"column_names\",\"type\":[{\"type\":\"array\",\"items\":\"bytes\"},\"null\"]},{\"name\":\"slice_range\",\"type\":[{\"type\":\"record\",\"name\":\"SliceRange\",\"fields\":[{\"name\":\"start\",\"type\":\"bytes\"},{\"name\":\"finish\",\"type\":\"bytes\"},{\"name\":\"reversed\",\"type\":\"boolean\"},{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"bitmasks\",\"type\":[{\"type\":\"array\",\"items\":\"bytes\"},\"null\"]}]},\"null\"]}]}");
  public org.apache.avro.generic.GenericArray<java.nio.ByteBuffer> column_names;
  public org.apache.cassandra.avro.SliceRange slice_range;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return column_names;
    case 1: return slice_range;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: column_names = (org.apache.avro.generic.GenericArray<java.nio.ByteBuffer>)value$; break;
    case 1: slice_range = (org.apache.cassandra.avro.SliceRange)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
