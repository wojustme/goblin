package com.wojustme.goblin.server.mysql.packet;


import com.wojustme.goblin.server.mysql.protocol.ServerStatusFlags;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class OkResponsePacket extends MysqlPacket {

  public final long affectedRows;
  public final long lastInsertId;
  public final int warnings;
  public final String info;
  public final Set<ServerStatusFlags> statusFlags = EnumSet.noneOf(ServerStatusFlags.class);
  public final String sessionStateChanges;

  public OkResponsePacket(Builder builder) {
    super(builder.sequenceId);

    affectedRows = builder.affectedRows;
    lastInsertId = builder.lastInsertId;

    warnings = builder.warnings;
    info = builder.info;

    statusFlags.addAll(builder.statusFlags);
    sessionStateChanges = builder.sessionStateChanges;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private int sequenceId;

    private long affectedRows;
    private long lastInsertId;

    private int warnings;
    private String info;

    private Set<ServerStatusFlags> statusFlags = EnumSet.noneOf(ServerStatusFlags.class);
    private String sessionStateChanges;

    public Builder sequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    public Builder affectedRows(long affectedRows) {
      this.affectedRows = affectedRows;
      return this;
    }

    public Builder lastInsertId(long lastInsertId) {
      this.lastInsertId = lastInsertId;
      return this;
    }

    public Builder addStatusFlags(ServerStatusFlags statusFlag, ServerStatusFlags... statusFlags) {
      this.statusFlags.add(statusFlag);
      Collections.addAll(this.statusFlags, statusFlags);
      return this;
    }

    public Builder addStatusFlags(Collection<ServerStatusFlags> statusFlags) {
      this.statusFlags.addAll(statusFlags);
      return this;
    }

    public Builder warnings(int warnings) {
      this.warnings = warnings;
      return this;
    }

    public Builder info(String info) {
      this.info = info;
      return this;
    }

    public Builder sessionStateChanges(String sessionStateChanges) {
      this.sessionStateChanges = sessionStateChanges;
      return this;
    }

    public OkResponsePacket build() {
      return new OkResponsePacket(this);
    }
  }
}
