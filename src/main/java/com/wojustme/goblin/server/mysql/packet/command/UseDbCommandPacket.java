package com.wojustme.goblin.server.mysql.packet.command;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.AffectSummaryResult;
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
  protected HandlerResult exec(SessionHandler sessionHandler) {
    Preconditions.checkArgument(
        sessionHandler.catalogService.listDatabases().contains(database),
        "Database: %s not exist.",
        database);
    sessionHandler.catalogService.setDefaultDb(database);
    sessionHandler.userSession.setDatabase(database);
    return new AffectSummaryResult(1);
  }

  @Override
  protected String genStr() {
    return new StringJoiner(", ", UseDbCommandPacket.class.getSimpleName() + "[", "]")
        .add("command=" + command)
        .add("database='" + database + "'")
        .toString();
  }
}
