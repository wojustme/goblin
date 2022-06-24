package com.wojustme.goblin.sql.util;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

public class SqlUtils {
    public static <T> T extractLiteral(SqlNode sqlNode, Class<T> clazz) {
        Preconditions.checkArgument(sqlNode instanceof SqlLiteral);
        final SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
        return (T)sqlLiteral.getValue();
    }
}
