package com.wojustme.goblin.sql.ex;


public class SqlParseException extends SqlRuntimeException {
  public SqlParseException(String msg, Object... otherMsg) {
    super(msg, otherMsg);
  }

  public SqlParseException(Throwable cause, String msg, Object... otherMsg) {
    super(cause, msg, otherMsg);
  }

  public SqlParseException(Throwable cause) {
    super(cause);
  }
}
