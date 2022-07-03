package com.wojustme.goblin.server.handler.result;

public class FailedResult extends HandlerResult {

  private final int errorCode;

  private final String message;

  public FailedResult(int errorCode, String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }
}
