package com.wojustme.goblin.server;

import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.server.mysql.MysqlProxy;
import com.wojustme.goblin.storage.DataStorageManager;
import com.wojustme.goblin.storage.impl.DataStorageManagerImpl;

public class GoblinStartup {

  private static final String DATA_DIR = "data";

  public static void main(String[] args) {
    // 1. startup storage service
    final DataStorageManager storageManager = new DataStorageManagerImpl(DATA_DIR);
    // 3. startup mysql proxy
    final MysqlProxy mysqlProxy = new MysqlProxy(3310);
    final GoblinContext goblinContext = new GoblinContext(storageManager);
    mysqlProxy.start(goblinContext);
  }
}
