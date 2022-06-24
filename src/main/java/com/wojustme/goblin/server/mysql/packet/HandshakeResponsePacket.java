package com.wojustme.goblin.server.mysql.packet;

import com.wojustme.goblin.server.mysql.protocol.CapabilityFlags;
import com.wojustme.goblin.server.mysql.protocol.CharacterSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HandshakeResponsePacket extends MysqlPacket {

  public final Set<CapabilityFlags> capabilityFlags;
  public final int maxPacketSize;
  public final CharacterSet characterSet;
  public final String username;
  public final String database;
  public final String authPluginName;
  public final ByteBuf authPluginData;
  public final Map<String, String> attributes;

  private HandshakeResponsePacket(Builder builder) {
    super(1);
    this.capabilityFlags = builder.capabilities;
    this.maxPacketSize = builder.maxPacketSize;
    this.characterSet = builder.characterSet;
    this.username = builder.username;
    this.database = builder.database;
    this.authPluginName = builder.authPluginName;
    this.authPluginData = builder.authPluginData;
    this.attributes = builder.attributes;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int maxPacketSize = Integer.MAX_VALUE;
    private CharacterSet characterSet = CharacterSet.DEFAULT;
    private String username;
    private String database;
    private String authPluginName;
    public final ByteBuf authPluginData = Unpooled.buffer();
    private Map<String, String> attributes = new HashMap<>();
    public final Set<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();

    public Builder maxPacketSize(int maxPacketSize) {
      this.maxPacketSize = maxPacketSize;
      return this;
    }

    public Builder characterSet(CharacterSet characterSet) {
      Objects.requireNonNull(characterSet, "characterSet can NOT be null");
      this.characterSet = characterSet;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder database(String database) {
      addCapabilities(CapabilityFlags.CLIENT_CONNECT_WITH_DB);
      this.database = database;
      return this;
    }

    public Builder authPluginName(String authPluginName) {
      addCapabilities(CapabilityFlags.CLIENT_PLUGIN_AUTH);
      this.authPluginName = authPluginName;
      return this;
    }

    public Builder addAuthData(ByteBuf buf, int length) {
      authPluginData.writeBytes(buf, length);
      return this;
    }

    private Builder addCapabilities(CapabilityFlags capabilityFlags) {
      this.capabilities.add(capabilityFlags);
      return this;
    }

    public Builder addCapabilities(EnumSet<CapabilityFlags> clientCapabilities) {
      this.capabilities.addAll(clientCapabilities);
      return this;
    }

    public Builder addAttribute(String key, String value) {
      attributes.put(key, value);
      return this;
    }

    public HandshakeResponsePacket build() {
      return new HandshakeResponsePacket(this);
    }
  }
}
