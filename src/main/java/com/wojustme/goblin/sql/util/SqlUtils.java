package com.wojustme.goblin.sql.util;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

public class SqlUtils {
  public static <T> T extractLiteral(SqlNode sqlNode, Class<T> clazz) {
    Preconditions.checkArgument(sqlNode instanceof SqlLiteral);
    final SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
    return (T) sqlLiteral.getValue();
  }

  public static boolean equalRelType(RelDataType r1, RelDataType r2) {
    if (r1.isNullable() != r2.isNullable()) {
      return false;
    }
    if (r1.getSqlTypeName() != r2.getSqlTypeName()) {
      return false;
    }
    return true;
  }
}
