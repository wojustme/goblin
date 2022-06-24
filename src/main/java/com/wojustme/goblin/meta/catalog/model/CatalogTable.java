package com.wojustme.goblin.meta.catalog.model;

import java.util.List;

/**
 * Table's Model
 */
public class CatalogTable {

  public final String dbName;

  public final String tableName;

  public final TableType tableType;

  public final List<CatalogColumn> columns;

  public CatalogTable(String dbName, String tableName, TableType tableType, List<CatalogColumn> columns) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.tableType = tableType;
    this.columns = columns;
  }
}
