package com.wojustme.goblin.server.mysql.packet;


import com.wojustme.goblin.server.mysql.protocol.ServerStatusFlags;

import java.util.EnumSet;
import java.util.Set;

public class EofResponsePacket extends MysqlPacket {

  public final int warnings;
  public final Set<ServerStatusFlags> statusFlags = EnumSet.noneOf(ServerStatusFlags.class);

  public EofResponsePacket(int sequenceId, int warnings) {
    super(sequenceId);
    this.warnings = warnings;
  }
}
