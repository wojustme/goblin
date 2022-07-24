package com.wojustme.goblin.meta.catalog.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.wojustme.goblin.common.NotSupportException;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.ex.MetaRuntimeException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InMemoryCatalogService implements CatalogService {

  private final String defaultDb;

  private final Map<String, CatalogDatabase> databases = new HashMap<>();
  private final Table<String, String, CatalogTable> tables = HashBasedTable.create();

  public InMemoryCatalogService(String defaultDb) {
    this.defaultDb = defaultDb;
    databases.put(defaultDb, new CatalogDatabase(defaultDb));
  }

  @Override
  public void setDefaultDb(String defaultDb) {
    throw new NotSupportException(
        "Not support method of `updateDb` for " + this.getClass().getName());
  }

  @Override
  public String defaultDb() {
    return defaultDb;
  }

  @Override
  public CatalogDatabase getDatabase(String name) {
    if (StringUtils.equals(name, defaultDb)) {
      return new CatalogDatabase(name);
    } else {
      return null;
    }
  }

  @Override
  public void createTable(CatalogTable catalogTable) {
    final CatalogTable existTable = getTable(catalogTable.dbName, catalogTable.tableName);
    if (existTable != null) {
      throw new MetaRuntimeException(
          "Table %s has existed in database %s", catalogTable.tableName, catalogTable.dbName);
    }
    tables.put(catalogTable.dbName, catalogTable.tableName, catalogTable);
  }

  @Override
  public boolean isSettingDatabase() {
    return true;
  }

  @Override
  public void createDatabase(CatalogDatabase catalogDatabase) {
    databases.put(catalogDatabase.name(), catalogDatabase);
  }

  @Override
  public Set<String> listDatabases() {
    return databases.keySet();
  }

  @Override
  public CatalogTable analyzeWithDefaultDB(List<String> names) {
    return null;
  }

  @Override
  public Set<String> listTables(String defaultDb) {
    return null;
  }

  @Override
  public CatalogTable getTable(String dbName, String tableName) {
    Preconditions.checkArgument(databases.containsKey(dbName));
    final CatalogTable catalogTable = tables.get(dbName, tableName);
    return catalogTable;
  }

  @Override
  public void dropTable(String dbName, String tableName) {
    tables.remove(dbName, tableName);
  }
}
