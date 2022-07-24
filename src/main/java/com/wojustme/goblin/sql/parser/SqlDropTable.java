package com.wojustme.goblin.sql.parser;

import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class SqlDropTable extends SqlDrop implements GoblinSqlDdl {

  /** "DROP TABLE" operator. */
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return null;
        }
      };

  private final SqlIdentifier tableIdentifier;

  public SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier tableIdentifier) {
    super(OPERATOR, pos, ifExists);
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  public void exec(CatalogService catalogService) {
      final Pair<String, String> tblNamespace = parseTableNamespace(catalogService, tableIdentifier);
      catalogService.dropTable(tblNamespace.getLeft(), tblNamespace.getRight());
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }
}
