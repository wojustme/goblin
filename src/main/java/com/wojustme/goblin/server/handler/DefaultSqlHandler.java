package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.handler.result.SucceedResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.ex.SqlRuntimeException;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * This handler for query statement, <br>
 * top kinds: {@link org.apache.calcite.sql.SqlSelect}, {@link SqlStdOperatorTable#UNION}
 */
public class DefaultSqlHandler extends AbstractSqlHandler {


  public DefaultSqlHandler(GoblinContext context, SqlPlanner sqlPlanner) {
    super(context, sqlPlanner);
  }

  @Override
  HandlerResult exec(SqlNode parsedNode) {
    if (parsedNode.toString().contains("DATABASE")) {
      final SucceedResult succeedResult = new SucceedResult();
      succeedResult.addField("DATABASE()", DataType.STRING);
      final String currentDb = sqlPlanner.getCatalogService().defaultDb();
      succeedResult.addRow(StringUtils.isEmpty(currentDb) ? null : currentDb);
      return succeedResult;
    }
    final SqlNode validatedNode = sqlPlanner.validate(parsedNode);
    final RelNode rel = sqlPlanner.convertRel(validatedNode);
    if (rel.explain().contains("@@")) {
      final SucceedResult succeedResult = new SucceedResult();
      succeedResult.addField("var", DataType.STRING);
      succeedResult.addRow("GoblinServer-0.1");
      return succeedResult;
    }

    final RelNode optimizedRel = sqlPlanner.optimize(rel);

    return switch (optimizedRel) {
      case TableModify tableModify -> handleInsert(sqlPlanner, tableModify);
      default -> throw new SqlRuntimeException("Goblin not support rel:\n%s", optimizedRel.explain());
    };
  }

  private HandlerResult handleInsert(SqlPlanner sqlPlanner, TableModify tableModify) {
    Preconditions.checkArgument(tableModify.isInsert(), "Goblin only support INSERT stat");
    final RelNode inputRel = tableModify.getInput();
    Preconditions.checkArgument(inputRel instanceof Values, "Input of INSERT must be value's operator.");
    final RelOptTable table = tableModify.getTable();
    final List<String> qualifiedName = table.getQualifiedName();
    final CatalogTable catalogTable = sqlPlanner.getCatalogService().analyzeWithDefaultDB(qualifiedName);
    Preconditions.checkArgument(catalogTable != null, "Table %s not exist.", qualifiedName);
    final Values values = (Values) inputRel;
    // column's count
    final int colCnt = catalogTable.columns.size();

    context.storageManager.writeDataBatch();
//    ImmutableList.builder()
//    for (ImmutableList<RexLiteral> tuple : values.tuples) {
//
//    }
//    context.storageManager.writeData();
    return null;
  }
}
