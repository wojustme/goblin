package com.wojustme.goblin.fun.calcite;

import org.apache.arrow.vector.holders.DateDayHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

class TypeConverter {

  static RelDataType convertType(
      RelDataTypeFactory typeFactory, Class<? extends ValueHolder> holderClass) {
    if (VarCharHolder.class.isAssignableFrom(holderClass)) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
    if (NullableVarCharHolder.class.isAssignableFrom(holderClass)) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    }

    if (DateDayHolder.class.isAssignableFrom(holderClass)) {
      return typeFactory.createSqlType(SqlTypeName.DATE);
    }
    if (NullableDateDayHolder.class.isAssignableFrom(holderClass)) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DATE), true);
    }
    throw new RuntimeException("Not support class: " + holderClass.getName());
  }
}
