package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.meta.catalog.model.DataType;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.server.handler.result.SucceedResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.parser.GoblinSqlShow;
import com.wojustme.goblin.sql.parser.SqlShow;
import org.apache.calcite.sql.SqlNode;

import java.util.Collection;

/**
 * This handler for ddl statement, <br>
 * top kinds: {@link SqlShow}
 */
public class ShowSqlHandler extends AbstractSqlHandler {

  public ShowSqlHandler(GoblinContext context, SqlPlanner sqlPlanner) {
    super(context, sqlPlanner);
  }

  @Override
  HandlerResult exec(SqlNode parsedNode) {
    Preconditions.checkArgument(parsedNode instanceof GoblinSqlShow);
    final GoblinSqlShow sqlShowNode = (GoblinSqlShow) parsedNode;
    final Collection<String> results = sqlShowNode.show(sqlPlanner.getCatalogService());
    final SucceedResult succeedResult = new SucceedResult();
    succeedResult.addField(sqlShowNode.tag().name(), DataType.STRING);
    results.forEach(succeedResult::addRow);
    return succeedResult;
  }
}
