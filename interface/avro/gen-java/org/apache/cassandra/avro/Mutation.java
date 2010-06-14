package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class Mutation extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Mutation\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"column_or_supercolumn\",\"type\":[{\"type\":\"record\",\"name\":\"ColumnOrSuperColumn\",\"fields\":[{\"name\":\"column\",\"type\":[{\"type\":\"record\",\"name\":\"Column\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"value\",\"type\":\"bytes\"},{\"name\":\"clock\",\"type\":{\"type\":\"record\",\"name\":\"Clock\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"context\",\"type\":[\"bytes\",\"null\"]}]}},{\"name\":\"ttl\",\"type\":[\"int\",\"null\"]}]},\"null\"]},{\"name\":\"super_column\",\"type\":[{\"type\":\"record\",\"name\":\"SuperColumn\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":\"Column\"}}]},\"null\"]}]},\"null\"]},{\"name\":\"deletion\",\"type\":[{\"type\":\"record\",\"name\":\"Deletion\",\"fields\":[{\"name\":\"clock\",\"type\":\"Clock\"},{\"name\":\"super_column\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"predicate\",\"type\":[{\"type\":\"record\",\"name\":\"SlicePredicate\",\"fields\":[{\"name\":\"column_names\",\"type\":[{\"type\":\"array\",\"items\":\"bytes\"},\"null\"]},{\"name\":\"slice_range\",\"type\":[{\"type\":\"record\",\"name\":\"SliceRange\",\"fields\":[{\"name\":\"start\",\"type\":\"bytes\"},{\"name\":\"finish\",\"type\":\"bytes\"},{\"name\":\"reversed\",\"type\":\"boolean\"},{\"name\":\"count\",\"type\":\"int\"},{\"name\":\"bitmasks\",\"type\":{\"type\":\"array\",\"items\":\"bytes\"}}]},\"null\"]}]},\"null\"]}]},\"null\"]}]}");
  public org.apache.cassandra.avro.ColumnOrSuperColumn column_or_supercolumn;
  public org.apache.cassandra.avro.Deletion deletion;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return column_or_supercolumn;
    case 1: return deletion;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: column_or_supercolumn = (org.apache.cassandra.avro.ColumnOrSuperColumn)value$; break;
    case 1: deletion = (org.apache.cassandra.avro.Deletion)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}