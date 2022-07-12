package com.wojustme.goblin.server.mysql.packet.command;

import com.wojustme.goblin.server.handler.SessionHandler;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.mysql.protocol.Command;

public class QuitCommandPacket extends CommandPacket {

  public final String someInfo;

  public QuitCommandPacket(int sequenceId, String someInfo) {
    super(sequenceId, Command.COM_QUIT);
    this.someInfo = someInfo;
  }

  @Override
  protected HandlerResult exec(SessionHandler sessionHandler) {
    return null;
  }

  @Override
  protected String genStr() {
    return null;
  }
}
