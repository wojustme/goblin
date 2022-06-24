package com.wojustme.goblin.server.mysql.protocol;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.EnumSet;
import java.util.Set;

/**
 * <a
 * href="https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags">CapabilityFlags</a>
 */
public enum CapabilityFlags {
  CLIENT_LONG_PASSWORD(0x00000001),
  CLIENT_FOUND_ROWS(0x00000002),
  CLIENT_LONG_FLAG(0x00000004),
  CLIENT_CONNECT_WITH_DB(0x00000008),
  CLIENT_NO_SCHEMA(0x00000010),
  CLIENT_COMPRESS(0x00000020),
  CLIENT_ODBC(0x00000040),
  CLIENT_LOCAL_FILES(0x00000080),
  CLIENT_IGNORE_SPACE(0x00000100),
  CLIENT_PROTOCOL_41(0x00000200),
  CLIENT_INTERACTIVE(0x00000400),
  CLIENT_SSL(0x00000800),
  CLIENT_IGNORE_SIGPIPE(0x00001000),
  CLIENT_TRANSACTIONS(0x00002000),
  CLIENT_RESERVED(0x00004000),
  CLIENT_SECURE_CONNECTION(0x00008000),
  CLIENT_MULTI_STATEMENTS(0x00010000),
  CLIENT_MULTI_RESULTS(0x00020000),
  CLIENT_PS_MULTI_RESULTS(0x00040000),
  CLIENT_PLUGIN_AUTH(0x00080000),
  CLIENT_CONNECT_ATTRS(0x00100000),
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000),
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000),
  CLIENT_SESSION_TRACK(0x00800000),
  ;
  public final int value;

  CapabilityFlags(int value) {
    this.value = value;
  }

  public static EnumSet<CapabilityFlags> getImplicitCapabilities() {
    return EnumSet.of(
        CapabilityFlags.CLIENT_LONG_PASSWORD,
        CapabilityFlags.CLIENT_FOUND_ROWS,
        CapabilityFlags.CLIENT_LONG_FLAG,
        CapabilityFlags.CLIENT_CONNECT_WITH_DB,
        CapabilityFlags.CLIENT_ODBC,
        CapabilityFlags.CLIENT_IGNORE_SPACE,
        CapabilityFlags.CLIENT_PROTOCOL_41,
        CapabilityFlags.CLIENT_INTERACTIVE,
        CapabilityFlags.CLIENT_IGNORE_SIGPIPE,
        CapabilityFlags.CLIENT_TRANSACTIONS,
        CapabilityFlags.CLIENT_SECURE_CONNECTION,
        CapabilityFlags.CLIENT_PLUGIN_AUTH,
        CapabilityFlags.CLIENT_CONNECT_ATTRS);
  }

  private static final AttributeKey<EnumSet<CapabilityFlags>> CAPABILITIES_KEY =
      AttributeKey.newInstance(CapabilityFlags.class.getName());

  public static void setCapabilitiesAttr(Channel channel, EnumSet<CapabilityFlags> capabilities) {
    final Attribute<EnumSet<CapabilityFlags>> attr = channel.attr(CAPABILITIES_KEY);
    attr.set(EnumSet.copyOf(capabilities));
  }

  public static EnumSet<CapabilityFlags> getCapabilitiesAttr(Channel channel) {
    final Attribute<EnumSet<CapabilityFlags>> attr = channel.attr(CAPABILITIES_KEY);
    if (attr.get() == null) {
      attr.set(getImplicitCapabilities());
    }
    return attr.get();
  }

  public static int encode(Set<CapabilityFlags> capabilities) {
    int vector = 0;
    for (CapabilityFlags elem : capabilities) {
      vector |= elem.value;
    }
    return vector;
  }

  public static EnumSet<CapabilityFlags> decode(int vector) {
    final EnumSet<CapabilityFlags> set = EnumSet.noneOf(CapabilityFlags.class);
    for (CapabilityFlags e : values()) {
      final int mask = 1 << e.value;
      if ((mask & vector) != 0) {
        set.add(e);
      }
    }
    return set;
  }
}
