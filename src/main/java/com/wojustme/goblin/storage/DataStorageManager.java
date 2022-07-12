package com.wojustme.goblin.storage;

import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;

import java.io.File;
import java.util.List;

/** Interface for data storage */
public interface DataStorageManager {

  /** Root directory */
  File rootDir();

  /** Operations of creating database, creating the directory of db_name<br> */
  void createDatabase(CatalogDatabase catalogDatabase);

  /**
   * Operations of creating table.<br>
   *
   * <pre>
   *     db_name
   *       |-table_name
   *         |-tbl.meta
   *         |-col1.data
   *         |-col2.data
   * </pre>
   *
   * - create the directory of db_name<br>
   * - create the directory of table_name<br>
   * - create the file of tbl.meta<br>
   * - create the files for each columns<br>
   */
  void createTable(CatalogTable catalogTable);

  /** Write data's blocks into file. */
  void writeData(List<DataBlock> blocks);

  List<DataBlock> readData(String... cols);

    void writeDataBatch();

}
