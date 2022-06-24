package com.wojustme.goblin.server.mysql.packet.command;


import com.wojustme.goblin.server.mysql.protocol.Command;

public class QueryCommandPacket extends CommandPacket {
  public final String query;

  public QueryCommandPacket(int sequenceId, String query) {
    super(sequenceId, Command.COM_QUERY);
    this.query = query;
  }
}
