package com.wojustme.goblin.sql.parser.ddl;

import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.sql.util.SqlUtils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/** {@link SqlNode} for {@code CREATE TABLE} statement. */
public class SqlCreateTable extends SqlCreate implements GoblinSqlDdl {
  /** "CREATE TABLE" operator. */
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlCreateTable(
              pos,
              SqlUtils.extractLiteral(operands[0], boolean.class),
              SqlUtils.extractLiteral(operands[1], boolean.class),
              (SqlIdentifier) operands[2],
              (SqlNodeList) operands[3],
              operands[4] == null ? null : SqlUtils.extractLiteral(operands[6], String.class));
        }
      };

  /** {@link SqlIdentifier} */
  private final SqlIdentifier tableIdentifier;

  /** {@link SqlColumn} */
  private final SqlNodeList columns;

  private final String comment;

  public SqlCreateTable(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier tableIdentifier,
      SqlNodeList columns,
      String comment) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.tableIdentifier = tableIdentifier;
    this.columns = columns;
    this.comment = comment;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(
        SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO),
        SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
        tableIdentifier,
        columns,
        comment == null ? null : SqlLiteral.createCharString(comment, SqlParserPos.ZERO));
  }

  @Override
  public void exec(CatalogService catalogService) {
  }
}
