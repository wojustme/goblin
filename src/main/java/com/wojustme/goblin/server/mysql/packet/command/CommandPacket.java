package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.mysql.packet.MysqlPacket;
import com.wojustme.goblin.server.mysql.protocol.Command;

public class CommandPacket extends MysqlPacket {
  public final Command command;

  public CommandPacket(int sequenceId, Command command) {
    super(sequenceId);
    this.command = command;
  }
}
