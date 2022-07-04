package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.DDLResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.mysql.protocol.Command;

import java.util.StringJoiner;

public class UseDbCommandPacket extends CommandPacket {
  public final String database;

  public UseDbCommandPacket(int sequenceId, String database) {
    super(sequenceId, Command.COM_INIT_DB);
    this.database = database;
  }

  @Override
  public HandlerResult handle(SessionHandler sessionHandler) {
    sessionHandler.catalogService.setDefaultDb(database);
    sessionHandler.userSession.setDatabase(database);
    return new DDLResult(1);
  }

  @Override
  protected String genStr() {
    return new StringJoiner(", ", UseDbCommandPacket.class.getSimpleName() + "[", "]")
        .add("command=" + command)
        .add("database='" + database + "'")
        .toString();
  }
}
