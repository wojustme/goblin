package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.FailedResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.mysql.packet.MysqlPacket;
import com.wojustme.goblin.server.mysql.protocol.Command;

public abstract class CommandPacket extends MysqlPacket {
  public final Command command;

  public CommandPacket(int sequenceId, Command command) {
    super(sequenceId);
    this.command = command;
  }

  public HandlerResult handle(SessionHandler sessionHandler) {
    try {
      return exec(sessionHandler);
    } catch (Throwable t) {
      return new FailedResult(808, t.getMessage());
    }
  }

  protected abstract HandlerResult exec(SessionHandler sessionHandler);

  protected abstract String genStr();

  @Override
  public String toString() {
    return genStr();
  }
}
