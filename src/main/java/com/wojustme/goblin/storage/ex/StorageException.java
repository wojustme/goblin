package com.wojustme.goblin.storage.ex;

import com.wojustme.goblin.common.GoblinRuntimeException;

public class StorageException extends GoblinRuntimeException {
    public StorageException(String msg, Object... otherMsg) {
        super(msg, otherMsg);
    }

    public StorageException(Throwable cause, String msg, Object... otherMsg) {
        super(cause, msg, otherMsg);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }
}
