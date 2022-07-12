package com.wojustme.goblin.server.handler;

import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import org.apache.calcite.sql.SqlNode;

public abstract class AbstractSqlHandler {

  protected final GoblinContext context;

  protected final SqlPlanner sqlPlanner;

  protected AbstractSqlHandler(GoblinContext context, SqlPlanner sqlPlanner) {
    this.context = context;
    this.sqlPlanner = sqlPlanner;
  }

  abstract HandlerResult exec(SqlNode parsedNode);
}
