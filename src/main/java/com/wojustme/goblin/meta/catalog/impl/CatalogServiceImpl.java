package com.wojustme.goblin.meta.catalog.impl;

import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;

public class CatalogServiceImpl implements CatalogService {

  private String defaultDb = "default";

  public CatalogServiceImpl(String defaultDb) {
    this.defaultDb = defaultDb;
  }

  @Override
  public void updateDb(String defaultDb) {
    this.defaultDb = defaultDb;
  }

  @Override
  public String defaultDb() {
    return defaultDb;
  }

  @Override
  public CatalogDatabase getDatabase(String name) {
    return null;
  }

  @Override
  public void createTable(CatalogTable catalogTable) {

  }

  @Override
  public CatalogTable getTable(String dbName, String tableName) {
    return null;
  }
}
