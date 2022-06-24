package com.wojustme.goblin.sql.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.wojustme.goblin.meta.catalog.model.CatalogColumn;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.sql.ex.SqlRuntimeException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class CalciteTable extends AbstractTable implements TranslatableTable {
  private final RelProtoDataType protoRowType;

  public CalciteTable(RelDataTypeFactory typeFactory, CatalogTable catalogTable) {
    final RelDataType rowRelDataType = convert(typeFactory, catalogTable);
    this.protoRowType = RelDataTypeImpl.proto(rowRelDataType);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return LogicalTableScan.create(cluster, relOptTable, ImmutableList.of());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  private static RelDataType convert(RelDataTypeFactory typeFactory, CatalogTable catalogTable) {
    final List<CatalogColumn> columns = catalogTable.columns;
    final ArrayList<String> fieldNames = Lists.newArrayListWithCapacity(columns.size());
    final ArrayList<RelDataType> fieldTypes = Lists.newArrayListWithCapacity(columns.size());
    columns.forEach(column -> {
      fieldNames.add(column.fieldName());
      RelDataType sqlType = switch (column.dataType()) {
        case BOOL -> typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        case BYTE -> typeFactory.createSqlType(SqlTypeName.TINYINT);
        case SHORT -> typeFactory.createSqlType(SqlTypeName.SMALLINT);
        case INT -> typeFactory.createSqlType(SqlTypeName.INTEGER);
        case LONG -> typeFactory.createSqlType(SqlTypeName.BIGINT);
        case FLOAT -> typeFactory.createSqlType(SqlTypeName.FLOAT);
        case DOUBLE -> typeFactory.createSqlType(SqlTypeName.DOUBLE);
        case STRING -> typeFactory.createSqlType(SqlTypeName.VARCHAR);
        default -> throw new SqlRuntimeException("SQL not support type " + column.dataType());
      };
      fieldTypes.add(typeFactory.createTypeWithNullability(sqlType, column.nullable()));
    });
    return typeFactory.createStructType(fieldTypes, fieldNames);
  }
}
