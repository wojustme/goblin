package com.wojustme.goblin.sql.ex;

import com.wojustme.goblin.common.GoblinRuntimeException;

public class SqlRuntimeException extends GoblinRuntimeException {
  public SqlRuntimeException(String msg, Object... otherMsg) {
    super(msg, otherMsg);
  }

  public SqlRuntimeException(Throwable cause, String msg, Object... otherMsg) {
    super(cause, msg, otherMsg);
  }

  public SqlRuntimeException(Throwable cause) {
    super(cause);
  }
}
