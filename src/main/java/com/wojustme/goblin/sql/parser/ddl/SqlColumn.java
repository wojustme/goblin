package com.wojustme.goblin.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
/** A {@link SqlNode} used to define a column when create table. */
public class SqlColumn extends SqlCall {
  private final SqlIdentifier columnIdentifier;
  private final SqlDataTypeSpec dataTypeSpec;
  private final boolean nullable;
  private final DefaultValue defaultValue;
  private final String comment;

  public SqlColumn(
      SqlParserPos pos,
      SqlIdentifier columnIdentifier,
      SqlDataTypeSpec dataTypeSpec,
      boolean nullable,
      DefaultValue defaultValue,
      String comment) {
    super(pos);
    this.columnIdentifier = columnIdentifier;
    this.dataTypeSpec = dataTypeSpec;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
    this.comment = comment;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    columnIdentifier.unparse(writer, leftPrec, rightPrec);
    dataTypeSpec.unparse(writer, leftPrec, rightPrec);
    if (!nullable) {
      writer.keyword("NOT NULL");
    }
    if (defaultValue != null) {
      writer.keyword("DEFAULT");
      writer.literal(defaultValue.value);
    }
    if (comment != null) {
      writer.keyword("COMMENT");
      writer.keyword("'" + comment + "'");
    }
  }

  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  public static class DefaultValue {
    private String value;

    private DefaultValue(String value) {
      this.value = value;
    }

    public static DefaultValue of(String value) {
      return new DefaultValue(value);
    }
  }
}
