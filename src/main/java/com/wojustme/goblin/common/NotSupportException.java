package com.wojustme.goblin.common;

public class NotSupportException extends GoblinRuntimeException {
    public NotSupportException(String msg, Object... otherMsg) {
        super(msg, otherMsg);
    }
}
