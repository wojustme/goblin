package com.wojustme.goblin.meta.catalog.model;

/** Data type for table's field. */
public enum DataType {
  BOOL(1),
  BYTE(1),
  SHORT(2),
  INT(4),
  LONG(8),
  FLOAT(4),
  DOUBLE(8),
  STRING(0),
  ;

  public final int byteSize;
  public final int bitSize;

  DataType(int byteSize) {
    this.byteSize = byteSize;
    this.bitSize = byteSize * 8;
  }

}
