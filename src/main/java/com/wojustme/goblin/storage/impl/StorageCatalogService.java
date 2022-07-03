package com.wojustme.goblin.storage.impl;

import com.google.common.collect.ImmutableSet;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.storage.DataStorageManager;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class StorageCatalogService implements CatalogService {
  private String defaultDb;

  private boolean settingDatabase = false;

  private final DataStorageManager storageManager;

  public StorageCatalogService(String defaultDb, DataStorageManager storageManager) {
    this.defaultDb = defaultDb;
    this.storageManager = storageManager;
    if (StringUtils.isNotEmpty(defaultDb)) {
      storageManager.createDatabase(new CatalogDatabase(defaultDb));
    }
  }

  @Override
  public boolean isSettingDatabase() {
    return settingDatabase;
  }

  @Override
  public void currentDb(String defaultDb) {
    this.settingDatabase = true;
    this.defaultDb = defaultDb;
  }

  @Override
  public String defaultDb() {
    return defaultDb;
  }

  @Override
  public void createDatabase(CatalogDatabase catalogDatabase) {
    storageManager.createDatabase(catalogDatabase);
  }

  @Override
  public CatalogDatabase getDatabase(String name) {
    final Set<String> databases = listDatabases();
    if (databases.contains(name)) {
      return new CatalogDatabase(name);
    } else {
      return null;
    }
  }

  @Override
  public Set<String> listDatabases() {
    final File dir = storageManager.rootDir();
    return Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .filter(File::isDirectory)
        .map(File::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public void createTable(CatalogTable catalogTable) {
    storageManager.createTable(catalogTable);
  }

  @Override
  public CatalogTable getTable(String dbName, String tableName) {
    return null;
  }

  @Override
  public Set<String> listTables(String defaultDb) {
    return ImmutableSet.of();
  }
}
