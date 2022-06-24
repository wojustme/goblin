package com.wojustme.goblin.server.mysql.protocol;

public enum ColumnType {
  MYSQL_TYPE_STRING(0xfe),
  ;
  public final int value;

  ColumnType(int value) {
    this.value = value;
  }
}
