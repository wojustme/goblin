package com.wojustme.goblin.common;

import com.wojustme.goblin.storage.DataStorageManager;

/** Single instance. */
public class GoblinContext {

  /** Storage manager */
  public final DataStorageManager storageManager;

  public GoblinContext(DataStorageManager storageManager) {
    this.storageManager = storageManager;
  }
}
