package com.wojustme.goblin.server.handler.rel;

import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.ex.SqlRuntimeException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;

/**
 * Handler for {@link org.apache.calcite.rel.RelNode}, such as: {@link
 * org.apache.calcite.rel.core.TableModify} or {@link org.apache.calcite.rel.core.Project}
 */
public abstract class RelHandler<REL extends RelNode> {

  protected final GoblinContext context;

  protected final SqlPlanner sqlPlanner;

  protected final REL relNode;

  protected RelHandler(GoblinContext context, SqlPlanner sqlPlanner, REL relNode) {
    this.context = context;
    this.sqlPlanner = sqlPlanner;
    this.relNode = relNode;
  }

  protected abstract HandlerResult exec();

  public HandlerResult go() {
    return exec();
  }

  public static RelHandler getHandler(GoblinContext context, SqlPlanner sqlPlanner, RelNode relNode) {
    return switch (relNode) {
      case TableModify tableModify -> new InsertHandler(context, sqlPlanner, tableModify);
      default -> new QueryHandler(context, sqlPlanner, relNode);
    };
  }
}
