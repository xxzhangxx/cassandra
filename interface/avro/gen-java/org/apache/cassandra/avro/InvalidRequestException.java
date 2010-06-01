package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class InvalidRequestException extends org.apache.avro.specific.SpecificExceptionBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"error\",\"name\":\"InvalidRequestException\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"why\",\"type\":[\"string\",\"null\"]}]}");
  public org.apache.avro.util.Utf8 why;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return why;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: why = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
