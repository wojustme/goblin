package com.wojustme.goblin.sql.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.sql.util.SqlUtils;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlCreateDatabase extends SqlCreate implements GoblinSqlDdl {

  /** "CREATE DATABASE" operator. */
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE DATABASE", SqlKind.CREATE_SCHEMA) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlCreateDatabase(
              pos,
              SqlUtils.extractLiteral(operands[0], boolean.class),
              SqlUtils.extractLiteral(operands[1], boolean.class),
              (SqlIdentifier) operands[2],
              operands[3] == null ? null : SqlUtils.extractLiteral(operands[3], String.class));
        }
      };

  private final SqlIdentifier dbIdentifier;

  private final String comment;

  public SqlCreateDatabase(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier dbIdentifier,
      String comment) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.dbIdentifier = dbIdentifier;
    this.comment = comment;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  @Override
  public void exec(CatalogService catalogService) {
    final ImmutableList<String> names = dbIdentifier.names;
    final int identifierNameSize = names.size();
    Preconditions.checkArgument(identifierNameSize == 1);
    final CatalogDatabase catalogDatabase = new CatalogDatabase(names.get(0));
    catalogService.createDatabase(catalogDatabase);
  }
}
