package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.mysql.protocol.Command;

import java.util.StringJoiner;

public class QueryCommandPacket extends CommandPacket {
  public final String query;

  public QueryCommandPacket(int sequenceId, String query) {
    super(sequenceId, Command.COM_QUERY);
    this.query = query;
  }

  @Override
  public HandlerResult handle(SessionHandler sessionHandler) {
    return sessionHandler.exec(query);
  }

  @Override
  protected String genStr() {
    return new StringJoiner(", ", QueryCommandPacket.class.getSimpleName() + "[", "]")
        .add("command=" + command)
        .add("query='" + query + "'")
        .toString();
  }
}
