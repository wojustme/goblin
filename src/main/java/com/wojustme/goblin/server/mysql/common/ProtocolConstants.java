package com.wojustme.goblin.server.mysql.common;

public final class ProtocolConstants {
  public static final String MYSQL_NATIVE_PASSWORD = "mysql_native_password";
  public static final int NUL_BYTE = 0x00;
  public static final int AUTH_PLUGIN_DATA_PART1_LEN = 8;
  public static final int HANDSHAKE_RESERVED_BYTES = 10;
  public static final int AUTH_PLUGIN_DATA_PART2_MIN_LEN = 13;
}
