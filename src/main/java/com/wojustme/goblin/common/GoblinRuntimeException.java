package com.wojustme.goblin.common;

import com.google.common.base.Strings;

/** Abstract runtime exception for GOBLIN */
public abstract class GoblinRuntimeException extends RuntimeException {

  public GoblinRuntimeException(String msg, Object... otherMsg) {
    super(Strings.lenientFormat(msg, otherMsg));
  }

  public GoblinRuntimeException(Throwable cause, String msg, Object... otherMsg) {
    super(Strings.lenientFormat(msg, otherMsg), cause);
  }

  public GoblinRuntimeException(Throwable cause) {
    super(cause);
  }
}
