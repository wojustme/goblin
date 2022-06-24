package com.wojustme.goblin.meta.ex;

import com.wojustme.goblin.common.GoblinRuntimeException;

public class MetaRuntimeException extends GoblinRuntimeException {

  public MetaRuntimeException(String msg, Object... otherMsg) {
    super(msg, otherMsg);
  }

  public MetaRuntimeException(Throwable cause, String msg, Object... otherMsg) {
    super(cause, msg, otherMsg);
  }

  public MetaRuntimeException(Throwable cause) {
    super(cause);
  }
}
