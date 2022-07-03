package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.server.handler.result.DDLResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlDdl;
import org.apache.calcite.sql.SqlNode;

/**
 * This handler for ddl statement, <br>
 * top kinds: {@link com.wojustme.goblin.sql.parser.ddl.SqlCreateTable}
 */
public class DDLSqlHandler extends AbstractSqlHandler {

  public DDLSqlHandler(SqlNode parsedNode) {
    super(parsedNode);
  }

  @Override
  HandlerResult exec(SqlPlanner sqlPlanner) {
    Preconditions.checkArgument(this.parsedNode instanceof GoblinSqlDdl);
    ((GoblinSqlDdl) parsedNode).exec(sqlPlanner.getCatalogService());
    return new DDLResult(1);
  }
}
