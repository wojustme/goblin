package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.AffectSummaryResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.mysql.protocol.Command;

import java.util.StringJoiner;

public class FieldListCommandPacket extends CommandPacket {
  public final String database;

  public FieldListCommandPacket(int sequenceId, String database) {
    super(sequenceId, Command.COM_FIELD_LIST);
    this.database = database;
  }

  @Override
  protected HandlerResult exec(SessionHandler sessionHandler) {
//    sessionHandler.catalogService.setDefaultDb(database);
//    sessionHandler.userSession.setDatabase(database);
    return new AffectSummaryResult(1);
  }

  @Override
  protected String genStr() {
    return new StringJoiner(", ", FieldListCommandPacket.class.getSimpleName() + "[", "]")
        .add("command=" + command)
        .add("database='" + database + "'")
        .toString();
  }
}
