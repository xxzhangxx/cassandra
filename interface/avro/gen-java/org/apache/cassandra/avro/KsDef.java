package org.apache.cassandra.avro;

@SuppressWarnings("all")
public class KsDef extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"KsDef\",\"namespace\":\"org.apache.cassandra.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"strategy_class\",\"type\":\"string\"},{\"name\":\"replication_factor\",\"type\":\"int\"},{\"name\":\"cf_defs\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"CfDef\",\"fields\":[{\"name\":\"keyspace\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"column_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"clock_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"comparator_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"subcomparator_type\",\"type\":[\"string\",\"null\"]},{\"name\":\"reconciler\",\"type\":[\"string\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"row_cache_size\",\"type\":[\"double\",\"null\"]},{\"name\":\"preload_row_cache\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"key_cache_size\",\"type\":[\"double\",\"null\"]}]}}}]}");
  public org.apache.avro.util.Utf8 name;
  public org.apache.avro.util.Utf8 strategy_class;
  public int replication_factor;
  public org.apache.avro.generic.GenericArray<org.apache.cassandra.avro.CfDef> cf_defs;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return strategy_class;
    case 2: return replication_factor;
    case 3: return cf_defs;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (org.apache.avro.util.Utf8)value$; break;
    case 1: strategy_class = (org.apache.avro.util.Utf8)value$; break;
    case 2: replication_factor = (java.lang.Integer)value$; break;
    case 3: cf_defs = (org.apache.avro.generic.GenericArray<org.apache.cassandra.avro.CfDef>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
