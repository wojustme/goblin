package com.wojustme.goblin.sql.parser.ddl;

import com.wojustme.goblin.sql.parser.ddl.SqlColumn;
import com.wojustme.goblin.sql.parser.ddl.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDdlNodes {

  /** Parser's Model for {@link SqlCreateTable} */
  public static SqlCreateTable createTable(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier tableIdentifier,
      SqlNodeList columns,
      String comment) {
    return new SqlCreateTable(pos, replace, ifNotExists, tableIdentifier, columns, comment);
  }

  /** Parser's Model for {@link SqlColumn} */
  public static SqlColumn column(
      SqlParserPos pos,
      SqlIdentifier columnIdentifier,
      SqlDataTypeSpec dataTypeSpec,
      boolean nullable,
      boolean isSetDefaultValue,
      String defaultValue,
      String comment) {
    final SqlColumn.DefaultValue defaultColumnValue =
        isSetDefaultValue ? SqlColumn.DefaultValue.of(defaultValue) : null;
    return new SqlColumn(
        pos, columnIdentifier, dataTypeSpec, nullable, defaultColumnValue, comment);
  }
}
