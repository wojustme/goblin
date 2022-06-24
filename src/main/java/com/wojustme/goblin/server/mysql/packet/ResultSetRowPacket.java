package com.wojustme.goblin.server.mysql.packet;

import java.util.List;

public class ResultSetRowPacket extends MysqlPacket {

  public final List<String> values;

  public ResultSetRowPacket(int sequenceId, List<String> values) {
    super(sequenceId);
    this.values = values;
  }
}
