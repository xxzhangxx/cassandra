package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class CfDef extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"CfDef\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"keyspace\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"column_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"clock_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"comparator_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"subcomparator_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"reconciler\",\"type\":[\"string\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"row_cache_size\",\"type\":[\"double\",\"null\"]},{\"name\":\"preload_row_cache\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"key_cache_size\",\"type\":[\"double\",\"null\"]},{\"name\":\"read_repair_chance\",\"type\":[\"double\",\"null\"]},{\"name\":\"gc_grace_seconds\",\"type\":[\"int\",\"null\"]},{\"name\":\"column_metadata\",\"type\":[{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ColumnDef\",\"fields\":[{\"name\":\"name\",\"type\":\"bytes\"},{\"name\":\"validation_class\",\"type\":\"string\"},{\"name\":\"index_type\",\"type\":[{\"type\":\"enum\",\"name\":\"IndexType\",\"symbols\":[\"KEYS\"]},\"null\"]},{\"name\":\"index_name\",\"type\":[\"string\",\"null\"]}]}},\"null\"]},{\"name\":\"id\",\"type\":[\"int\",\"null\"]}]}");
  public org.apache.avro.util.Utf8 keyspace;
  public org.apache.avro.util.Utf8 name;
  public org.apache.avro.util.Utf8 column_type;
  public org.apache.avro.util.Utf8 clock_type;
  public org.apache.avro.util.Utf8 comparator_type;
  public org.apache.avro.util.Utf8 subcomparator_type;
  public org.apache.avro.util.Utf8 reconciler;
  public org.apache.avro.util.Utf8 comment;
  public java.lang.Double row_cache_size;
  public java.lang.Boolean preload_row_cache;
  public java.lang.Double key_cache_size;
  public java.lang.Double read_repair_chance;
  public java.lang.Integer gc_grace_seconds;
  public org.apache.avro.generic.GenericArray<org.apache.cassandra.avro.ColumnDef> column_metadata;
  public java.lang.Integer id;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return keyspace;
    case 1: return name;
    case 2: return column_type;
    case 3: return clock_type;
    case 4: return comparator_type;
    case 5: return subcomparator_type;
    case 6: return reconciler;
    case 7: return comment;
    case 8: return row_cache_size;
    case 9: return preload_row_cache;
    case 10: return key_cache_size;
    case 11: return read_repair_chance;
    case 12: return gc_grace_seconds;
    case 13: return column_metadata;
    case 14: return id;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: keyspace = (org.apache.avro.util.Utf8)value$; break;
    case 1: name = (org.apache.avro.util.Utf8)value$; break;
    case 2: column_type = (org.apache.avro.util.Utf8)value$; break;
    case 3: clock_type = (org.apache.avro.util.Utf8)value$; break;
    case 4: comparator_type = (org.apache.avro.util.Utf8)value$; break;
    case 5: subcomparator_type = (org.apache.avro.util.Utf8)value$; break;
    case 6: reconciler = (org.apache.avro.util.Utf8)value$; break;
    case 7: comment = (org.apache.avro.util.Utf8)value$; break;
    case 8: row_cache_size = (java.lang.Double)value$; break;
    case 9: preload_row_cache = (java.lang.Boolean)value$; break;
    case 10: key_cache_size = (java.lang.Double)value$; break;
    case 11: read_repair_chance = (java.lang.Double)value$; break;
    case 12: gc_grace_seconds = (java.lang.Integer)value$; break;
    case 13: column_metadata = (org.apache.avro.generic.GenericArray<org.apache.cassandra.avro.ColumnDef>)value$; break;
    case 14: id = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
