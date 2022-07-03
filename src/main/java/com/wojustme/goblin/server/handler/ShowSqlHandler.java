package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.result.DDLResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.handler.result.SucceedResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlDdl;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlShow;
import org.apache.calcite.sql.SqlNode;

import java.util.Collection;
import java.util.List;

/**
 * This handler for ddl statement, <br>
 * top kinds: {@link com.wojustme.goblin.sql.parser.ddl.SqlShow}
 */
public class ShowSqlHandler extends AbstractSqlHandler {

  public ShowSqlHandler(SqlNode parsedNode) {
    super(parsedNode);
  }

  @Override
  HandlerResult exec(SqlPlanner sqlPlanner) {
    Preconditions.checkArgument(this.parsedNode instanceof GoblinSqlShow);
    final GoblinSqlShow sqlShowNode = (GoblinSqlShow) parsedNode;
    final Collection<String> results = sqlShowNode.show(sqlPlanner.getCatalogService());
    final SucceedResult succeedResult = new SucceedResult();
    succeedResult.addField(sqlShowNode.tag().name(), DataType.STRING);
    results.forEach(succeedResult::addRow);
    return succeedResult;
  }
}
