package com.wojustme.goblin.meta.catalog.impl;

import com.wojustme.goblin.common.NotSupportException;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.ex.MetaRuntimeException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class InMemoryCatalogService implements CatalogService {

  private final String defaultDb;

  private final Map<String, CatalogTable> tables = new HashMap<>();

  public InMemoryCatalogService(String defaultDb) {
    this.defaultDb = defaultDb;
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
    tables.put(catalogTable.tableName, catalogTable);
  }

  @Override
  public CatalogTable getTable(String dbName, String tableName) {
    final CatalogTable catalogTable = tables.get(tableName);
    return catalogTable;
  }
}
