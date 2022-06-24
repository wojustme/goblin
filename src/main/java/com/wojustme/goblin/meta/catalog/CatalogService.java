package com.wojustme.goblin.meta.catalog;

import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;

/** Catalog service */
public interface CatalogService {

  /** set default database's name */
  void updateDb(String defaultDb);

  /** Get default database's name */
  String defaultDb();

  /** Get a database by name. It returns null, if not found. */
  CatalogDatabase getDatabase(String name);

  void createTable(CatalogTable catalogTable);

  /** Get a table by db-name and table-name. It returns null, if not found. */
  CatalogTable getTable(String dbName, String tableName);
}
