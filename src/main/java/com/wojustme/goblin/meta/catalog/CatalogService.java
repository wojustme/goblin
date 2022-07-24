package com.wojustme.goblin.meta.catalog;

import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;

import java.util.List;
import java.util.Set;

/** Catalog service */
public interface CatalogService {

  /** Whether setting database */
  boolean isSettingDatabase();
  /** set default database's name */
  void setDefaultDb(String defaultDb);

  /** Get default database's name */
  String defaultDb();

  void createDatabase(CatalogDatabase catalogDatabase);

  /** Get a database by name. It returns null, if not found. */
  CatalogDatabase getDatabase(String name);

  /** List all database's names */
  Set<String> listDatabases();

  void createTable(CatalogTable catalogTable);

  /** Get a table by db-name and table-name. It returns null, if not found. */
  CatalogTable getTable(String dbName, String tableName);

  /** Analyze and get table with default-db. */
  CatalogTable analyzeWithDefaultDB(List<String> names);

  /** List all table's names in target database */
  Set<String> listTables(String defaultDb);

    void dropTable(String dbName, String tableName);
}
