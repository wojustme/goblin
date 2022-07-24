package com.wojustme.goblin.storage.impl;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.model.CatalogColumn;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.meta.catalog.model.TableType;
import com.wojustme.goblin.storage.ex.StorageException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Model for describing table meta. */
public class TableDesc {

  private static final String FILE_NAME = "tbl.meta";

  private static final String COL_INFO_SEQ = ",";

  private CatalogTable catalogTable;

  public final File tableDir;
  public final File metaFile;

  public TableDesc(File dir) {
    this.tableDir = dir;
    this.metaFile = FileUtils.getFile(dir, FILE_NAME);
  }

  public void setCatalogTable(CatalogTable catalogTable) {
    this.catalogTable = catalogTable;
  }

  public CatalogTable getCatalogTable() {
    return catalogTable;
  }

  public void writeMeta() {
    final List<String> colInfoList =
        catalogTable.columns.stream()
            .map(
                col ->
                    col.fieldName() + COL_INFO_SEQ + col.dataType() + COL_INFO_SEQ + col.nullable())
            .toList();
    try {
      FileUtils.writeLines(metaFile, StandardCharsets.UTF_8.name(), colInfoList);
    } catch (IOException e) {
      throw new StorageException(
          e, "Write table %s meta into file failed.", catalogTable.tableName);
    }
  }

  public void readMeta() {
    try {
      final List<String> colLines = FileUtils.readLines(metaFile, StandardCharsets.UTF_8);
      final File tblDir = metaFile.getParentFile();
      final File dbDir = tblDir.getParentFile();
      final List<CatalogColumn> columns =
          colLines.stream()
              .map(
                  colStr -> {
                    final String[] split = StringUtils.split(colStr, COL_INFO_SEQ);
                    // Size of arr must be 3.
                    Preconditions.checkArgument(split.length == 3);
                    final String colName = split[0];
                    final String dataType = split[1];
                    final String nullable = split[2];
                    return new CatalogColumn(
                        colName, DataType.valueOf(dataType), Boolean.parseBoolean(nullable));
                  })
              .toList();
      this.catalogTable =
          new CatalogTable(dbDir.getName(), tblDir.getName(), TableType.ENTITY, columns);
    } catch (IOException e) {
      throw new StorageException(e, "Read table %s meta from file failed.", metaFile);
    }
  }
}
