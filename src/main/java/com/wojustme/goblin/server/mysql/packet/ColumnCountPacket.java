package com.wojustme.goblin.server.mysql.packet;

public class ColumnCountPacket extends MysqlPacket {

  public final int fieldCount;

  public ColumnCountPacket(int sequenceId, int fieldCount) {
    super(sequenceId);
    this.fieldCount = fieldCount;
  }
}
