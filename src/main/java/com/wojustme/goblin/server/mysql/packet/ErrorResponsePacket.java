package com.wojustme.goblin.server.mysql.packet;

import java.nio.charset.StandardCharsets;

public class ErrorResponsePacket extends MysqlPacket {
  public final int errorNumber;
  public final byte[] sqlState;
  public final String message;

  private ErrorResponsePacket(int sequenceId, int errorNumber, byte[] sqlState, String message) {
    super(sequenceId);
    this.errorNumber = errorNumber;
    this.sqlState = sqlState;
    this.message = message;
  }

  public static ErrorResponsePacket createOf(int sequenceId, int errorNumber, String message) {
    return new ErrorResponsePacket(
        sequenceId, errorNumber, "HY000".getBytes(StandardCharsets.UTF_8), message);
  }
}
