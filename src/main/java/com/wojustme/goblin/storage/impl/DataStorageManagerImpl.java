package com.wojustme.goblin.storage.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.common.FileUtils;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.storage.DataBlock;
import com.wojustme.goblin.storage.DataStorageManager;
import com.wojustme.goblin.storage.ex.StorageException;

import java.io.File;
import java.util.List;

/** Basic impl for data storage */
public class DataStorageManagerImpl implements DataStorageManager {

  private final String rootDirPath;
  private final File rootDir;

  public DataStorageManagerImpl(String rootDirPath) {
    this.rootDirPath = rootDirPath;
    this.rootDir = org.apache.commons.io.FileUtils.getFile(rootDirPath);
    // Check whether it exists, if not exist, goblin create directory.
    if (!FileUtils.dirExist(rootDir)) {
      FileUtils.forceMkdir(rootDir);
    }
  }

  @Override
  public File rootDir() {
    return rootDir;
  }

  @Override
  public void createDatabase(CatalogDatabase catalogDatabase) {
    final File dbDir = buildFileByPath(catalogDatabase.name());
    if (!FileUtils.dirExist(dbDir)) {
      FileUtils.forceMkdir(dbDir);
    }
  }

  @Override
  public void createTable(CatalogTable catalogTable) {
    // Handle for database
    final File dbDir = buildFileByPath(catalogTable.dbName);
    Preconditions.checkArgument(
        FileUtils.dirExist(dbDir),
        "You should create database: %s before creating table: %s",
        catalogTable.dbName,
        catalogTable.tableName);
    // Handle for table
    final File tableDir = buildFileByPath(catalogTable.dbName, catalogTable.tableName);
    if (FileUtils.dirExist(tableDir)) {
      throw new StorageException("Storage of table %s has existed.", catalogTable.tableName);
    }
    FileUtils.forceMkdir(tableDir);

    // Write table's schema into tbl.meta
    final TableMetaDescription tableMetaDescription = new TableMetaDescription(tableDir);
    tableMetaDescription.setCatalogTable(catalogTable);
    tableMetaDescription.writeMeta();
  }

  private File buildFileByPath(String... names) {
    final String realPath = rootDirPath + String.join(File.separator, names);
    return org.apache.commons.io.FileUtils.getFile(rootDir, realPath);
  }

  @Override
  public void writeData(List<DataBlock> blocks) {}

  @Override
  public List<DataBlock> readData(String... cols) {
    return null;
  }
}
