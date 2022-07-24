package com.wojustme.goblin.server.handler.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.meta.catalog.model.CatalogColumn;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.result.AffectSummaryResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.storage.TableIO;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class InsertHandler extends RelHandler<TableModify> {

  InsertHandler(GoblinContext context, SqlPlanner sqlPlanner, TableModify relNode) {
    super(context, sqlPlanner, relNode);
  }

  @Override
  protected HandlerResult exec() {
    Preconditions.checkArgument(relNode.isInsert(), "Goblin only support INSERT stat");
    final RelNode inputRel = relNode.getInput();
    Preconditions.checkArgument(
        inputRel instanceof Values, "Input of INSERT must be value's operator.");
    final RelOptTable table = relNode.getTable();
    final List<String> qualifiedName = table.getQualifiedName();
    final CatalogTable catalogTable =
        sqlPlanner.getCatalogService().analyzeWithDefaultDB(qualifiedName);
    Preconditions.checkArgument(catalogTable != null, "Table %s not exist.", qualifiedName);
    final Values values = (Values) inputRel;
    final List<FieldVector> valueVectors = transformToArrow(catalogTable, values);

    final TableIO tableIO = context.storageManager().getTableIO(catalogTable);
    tableIO.write(valueVectors);
    return new AffectSummaryResult(values.tuples.size());
  }

  private List<FieldVector> transformToArrow(CatalogTable catalogTable, Values values) {
    final ImmutableList<ImmutableList<RexLiteral>> tuples = values.tuples;
    final int rowNum = tuples.size();
    final int columnCount = tuples.get(0).size();
    final RexLiteral[][] columnarDataArr = new RexLiteral[columnCount][rowNum];

    for (int i = 0; i < tuples.size(); i++) {
      final ImmutableList<RexLiteral> rowData = tuples.get(i);
      Preconditions.checkArgument(rowData.size() == catalogTable.columns.size());
      for (int j = 0; j < rowData.size(); j++) {
        columnarDataArr[j][i] = rowData.get(j);
      }
    }

    final ImmutableList.Builder<FieldVector> builder = ImmutableList.builder();
    for (int i = 0; i < catalogTable.columns.size(); i++) {
      final RexLiteral[] columnarData = columnarDataArr[i];
      builder.add(buildValueVector(catalogTable.columns.get(i), rowNum, columnarData));
    }
    return builder.build();
  }

  private FieldVector buildValueVector(
      CatalogColumn column, int rowNum, RexLiteral[] columnarData) {
    final BufferAllocator allocator = new RootAllocator();
    final DataType dataType = column.dataType();
    switch (dataType) {
      case BOOL:
        final TinyIntVector tinyIntVector = new TinyIntVector(column.fieldName(), allocator);
        tinyIntVector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Boolean value = extractValue(columnarData[i], Boolean.class);
          if (value == null) {
            tinyIntVector.setNull(i);
          } else {
            tinyIntVector.set(i, value ? 1 : 0);
          }
        }
        tinyIntVector.setValueCount(rowNum);
        return tinyIntVector;
      case SHORT:
        final SmallIntVector smallIntVector = new SmallIntVector(column.fieldName(), allocator);
        smallIntVector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Short value = extractValue(columnarData[i], Short.class);
          if (value == null) {
            smallIntVector.setNull(i);
          } else {
            smallIntVector.set(i, value);
          }
        }
        smallIntVector.setValueCount(rowNum);
        return smallIntVector;
      case INT:
        final IntVector intVector = new IntVector(column.fieldName(), allocator);
        intVector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Integer value = extractValue(columnarData[i], Integer.class);
          if (value == null) {
            intVector.setNull(i);
          } else {
            intVector.set(i, value);
          }
        }
        intVector.setValueCount(rowNum);
        return intVector;
      case LONG:
        final BigIntVector bigIntVector = new BigIntVector(column.fieldName(), allocator);
        bigIntVector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Long value = extractValue(columnarData[i], Long.class);
          if (value == null) {
            bigIntVector.setNull(i);
          } else {
            bigIntVector.set(i, value);
          }
        }
        bigIntVector.setValueCount(rowNum);
        return bigIntVector;
      case FLOAT:
        final Float4Vector float4Vector = new Float4Vector(column.fieldName(), allocator);
        float4Vector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Float value = extractValue(columnarData[i], Float.class);
          if (value == null) {
            float4Vector.setNull(i);
          } else {
            float4Vector.set(i, value);
          }
        }
        float4Vector.setValueCount(rowNum);
        return float4Vector;
      case DOUBLE:
        final Float8Vector float8Vector = new Float8Vector(column.fieldName(), allocator);
        float8Vector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final Double value = extractValue(columnarData[i], Double.class);
          if (value == null) {
            float8Vector.setNull(i);
          } else {
            float8Vector.set(i, value);
          }
        }
        float8Vector.setValueCount(rowNum);
        return float8Vector;
      case STRING:
        final VarCharVector varCharVector = new VarCharVector(column.fieldName(), allocator);
        varCharVector.allocateNew(rowNum);
        for (int i = 0; i < columnarData.length; i++) {
          final String value = extractValue(columnarData[i], String.class);
          if (value == null) {
            varCharVector.setNull(i);
          } else {
            varCharVector.set(i, value.getBytes(StandardCharsets.UTF_8));
          }
        }
        varCharVector.setValueCount(rowNum);
        return varCharVector;
      default:
        throw new RuntimeException("Not support " + dataType + " for transforming to arrow");
    }
  }

  private <T> T extractValue(RexLiteral rexLiteral, Class<T> clazz) {
    if (rexLiteral.isNull()) {
      return null;
    } else {
      return rexLiteral.getValueAs(clazz);
    }
  }
}
