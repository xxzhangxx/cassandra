package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class SliceRange extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"SliceRange\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"start\",\"type\":\"bytes\"},{\"name\":\"finish\",\"type\":\"bytes\"},{\"name\":\"reversed\",\"type\":\"boolean\"},{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"bitmasks\",\"type\":{\"type\":\"array\",\"items\":\"bytes\"}}]}");
  public java.nio.ByteBuffer start;
  public java.nio.ByteBuffer finish;
  public boolean reversed;
  public int count;
  public org.apache.avro.generic.GenericArray<java.nio.ByteBuffer> bitmasks;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return start;
    case 1: return finish;
    case 2: return reversed;
    case 3: return count;
    case 4: return bitmasks;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: start = (java.nio.ByteBuffer)value$; break;
    case 1: finish = (java.nio.ByteBuffer)value$; break;
    case 2: reversed = (java.lang.Boolean)value$; break;
    case 3: count = (java.lang.Integer)value$; break;
    case 4: bitmasks = (org.apache.avro.generic.GenericArray<java.nio.ByteBuffer>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
