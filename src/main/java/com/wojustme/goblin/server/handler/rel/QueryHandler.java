package com.wojustme.goblin.server.handler.rel;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.handler.result.SucceedResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.storage.TableIO;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class QueryHandler extends RelHandler<RelNode> {

  public QueryHandler(GoblinContext context, SqlPlanner sqlPlanner, RelNode relNode) {
    super(context, sqlPlanner, relNode);
  }

  @Override
  protected HandlerResult exec() {
    final RelNode optimize = sqlPlanner.optimize(relNode);
    Preconditions.checkArgument(optimize instanceof TableScan);
    final TableScan tableScan = (TableScan) optimize;
    final RelOptTable table = tableScan.getTable();
    final List<String> qualifiedName = table.getQualifiedName();
    final CatalogTable catalogTable =
        sqlPlanner.getCatalogService().analyzeWithDefaultDB(qualifiedName);

    final TableIO tableIO = context.storageManager().getTableIO(catalogTable);
    //    tableIO.readAllColumns();
    final SucceedResult succeedResult = new SucceedResult();
    succeedResult.addField("hello", DataType.STRING);
    return succeedResult;
  }
}
