package com.wojustme.goblin.sql.parser;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class SqlShow extends SqlCall implements GoblinSqlShow {

  /** "SHOW DATABASES" or "SHOW TABLES" operator. */
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW", SqlKind.OTHER_DDL) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return null;
        }
      };

  protected final ShowTag showTag;

  public SqlShow(SqlParserPos pos, String showTagStr) {
    super(pos);
    this.showTag = ShowTag.valueOf(showTagStr.toUpperCase(Locale.ROOT));
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
  public ShowTag tag() {
    return showTag;
  }

  @Override
  public Collection<String> show(CatalogService catalogService) {
    switch (showTag) {
      case DATABASES:
        return catalogService.listDatabases();
      case TABLES:
        Preconditions.checkArgument(
            catalogService.isSettingDatabase(),
            "Please set default database, before executing sql stat of 'SHOW TABLES'");
        return catalogService.listTables(catalogService.defaultDb());
      default:
        throw new RuntimeException("Not support tag: " + showTag);
    }
  }

  public enum ShowTag {
    DATABASES,
    TABLES,
    ;
  }
}
