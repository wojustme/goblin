package com.wojustme.goblin.sql.parser;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlUseDatabase extends SqlCall implements GoblinSqlDdl {

  private final SqlIdentifier dbIdentifier;

  public SqlUseDatabase(SqlParserPos pos, SqlIdentifier dbIdentifier) {
    super(pos);
    this.dbIdentifier = dbIdentifier;
  }

  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  @Override
  public void exec(CatalogService catalogService) {
    Preconditions.checkArgument(dbIdentifier.names.size() == 1);
    final String dbName = dbIdentifier.names.get(0);
    final CatalogDatabase database = catalogService.getDatabase(dbName);
    Preconditions.checkArgument(database != null, "Database: %s not exist", dbName);
    catalogService.setDefaultDb(dbName);
  }
}
