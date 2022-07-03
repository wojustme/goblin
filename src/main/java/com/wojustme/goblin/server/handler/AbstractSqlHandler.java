package com.wojustme.goblin.server.handler;

import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import org.apache.calcite.sql.SqlNode;

public abstract class AbstractSqlHandler {

  /** Parsed {@link SqlNode} */
  protected final SqlNode parsedNode;

  protected AbstractSqlHandler(SqlNode parsedNode) {
    this.parsedNode = parsedNode;
  }

  abstract HandlerResult exec(SqlPlanner sqlPlanner);
}
