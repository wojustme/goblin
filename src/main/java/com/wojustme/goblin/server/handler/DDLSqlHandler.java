package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.server.handler.result.AffectSummaryResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.parser.GoblinSqlDdl;
import com.wojustme.goblin.sql.parser.SqlCreateTable;
import org.apache.calcite.sql.SqlNode;

/**
 * This handler for ddl statement, <br>
 * top kinds: {@link SqlCreateTable}
 */
public class DDLSqlHandler extends AbstractSqlHandler {

  public DDLSqlHandler(GoblinContext context, SqlPlanner sqlPlanner) {
    super(context, sqlPlanner);
  }

  @Override
  HandlerResult exec(SqlNode parsedNode) {
    Preconditions.checkArgument(parsedNode instanceof GoblinSqlDdl);
    ((GoblinSqlDdl) parsedNode).exec(sqlPlanner.getCatalogService());
    return new AffectSummaryResult(1);
  }
}
