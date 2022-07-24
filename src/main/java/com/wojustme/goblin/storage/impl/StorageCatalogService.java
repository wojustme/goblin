package com.wojustme.goblin.storage.impl;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.storage.DataStorageManager;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
  public void setDefaultDb(String defaultDb) {
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
    final File rootDir = storageManager.rootDir();
    if (!dirExists(rootDir)) {
      throw new RuntimeException("Dir: " + rootDir.getName() + " not exist or not a dir.");
    }
    return Arrays.stream(Objects.requireNonNull(rootDir.listFiles()))
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
    Preconditions.checkArgument(
        dbName != null && tableName != null, "Database and table couldn't be NULL.");
    final File tblDir = FileUtils.getFile(storageManager.rootDir(), dbName, tableName);
    if (dirExists(tblDir)) {
      final TableDesc tableDesc = new TableDesc(tblDir);
      tableDesc.readMeta();
      return tableDesc.getCatalogTable();
    } else {
      return null;
    }
  }

  @Override
  public CatalogTable analyzeWithDefaultDB(List<String> names) {
    final int size = CollectionUtils.size(names);
    Preconditions.checkArgument(size == 1 || size == 2);
    if (size == 1) {
      return getTable(defaultDb(), names.get(0));
    } else {
      return getTable(names.get(0), names.get(1));
    }
  }

  @Override
  public Set<String> listTables(String defaultDb) {
    final File dbDir = FileUtils.getFile(storageManager.rootDir(), defaultDb);
    if (!dirExists(dbDir)) {
      throw new RuntimeException("Dir: " + dbDir.getName() + " not exist or not a dir.");
    }
    return Arrays.stream(Objects.requireNonNull(dbDir.listFiles()))
        .filter(File::isDirectory)
        .map(File::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public void dropTable(String dbName, String tableName) {
    final File tblDir = FileUtils.getFile(storageManager.rootDir(), dbName, tableName);
    try {
      FileUtils.forceDeleteOnExit(tblDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean dirExists(File dir) {
    if (!dir.exists()) {
      return false;
    }
    return dir.isDirectory();
  }
}
