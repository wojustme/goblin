package com.wojustme.goblin.server.mysql.packet;

public abstract class MysqlPacket {

  public final int sequenceId;

  protected MysqlPacket(int sequenceId) {
    this.sequenceId = sequenceId;
  }
}
