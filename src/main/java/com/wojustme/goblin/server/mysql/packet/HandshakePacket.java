package com.wojustme.goblin.server.mysql.packet;

import com.wojustme.goblin.server.mysql.protocol.CapabilityFlags;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import com.wojustme.goblin.server.mysql.protocol.ServerStatusFlags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AsciiString;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public class HandshakePacket extends MysqlPacket {

  public final int protocolVersion;
  public final AsciiString serverVersion;
  public final int connectionId;
  public final Set<CapabilityFlags> capabilities;
  public final CharacterSet characterSet;
  public final Set<ServerStatusFlags> serverStatus;
  public final String authPluginName;
  public final ByteBuf authPluginData;

  private HandshakePacket(Builder builder) {
    super(0);
    this.protocolVersion = builder.protocolVersion;
    this.serverVersion = AsciiString.of(builder.serverVersion);
    this.connectionId = builder.connectionId;
    this.capabilities = builder.capabilities;
    this.characterSet = builder.characterSet;
    this.serverStatus = builder.serverStatus;
    this.authPluginName = builder.authPluginName;
    this.authPluginData = builder.authPluginData;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private int protocolVersion = 10;
    private String serverVersion;
    private int connectionId = -1;
    private CharacterSet characterSet = CharacterSet.DEFAULT;
    private final Set<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();
    private Set<ServerStatusFlags> serverStatus = EnumSet.noneOf(ServerStatusFlags.class);
    private String authPluginName;
    private final ByteBuf authPluginData = Unpooled.buffer();

    public Builder protocolVersion(int protocolVerison) {
      this.protocolVersion = protocolVerison;
      return this;
    }

    public Builder serverVersion(String serverVersion) {
      this.serverVersion = serverVersion;
      return this;
    }

    public Builder connectionId(int connectionId) {
      this.connectionId = connectionId;
      return this;
    }

    public Builder characterSet(CharacterSet characterSet) {
      this.characterSet = characterSet == null ? CharacterSet.DEFAULT : characterSet;
      return this;
    }

    public Builder addCapabilities(EnumSet<CapabilityFlags> capabilities) {
      this.capabilities.addAll(capabilities);
      return this;
    }

    public Builder addServerStatus(
        ServerStatusFlags serverStatus, ServerStatusFlags... serverStatuses) {
      this.serverStatus.add(serverStatus);
      Collections.addAll(this.serverStatus, serverStatuses);
      return this;
    }

    public Builder addServerStatus(Collection<ServerStatusFlags> serverStatus) {
      this.serverStatus.addAll(serverStatus);
      return this;
    }

    public Builder authPluginName(String authPluginName) {
      capabilities.add(CapabilityFlags.CLIENT_PLUGIN_AUTH);
      this.authPluginName = authPluginName;
      return this;
    }

    public Builder addAuthData(byte[] bytes) {
      authPluginData.writeBytes(bytes);
      return this;
    }

    public HandshakePacket build() {
      return new HandshakePacket(this);
    }
  }
}
