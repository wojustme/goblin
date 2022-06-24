package com.wojustme.goblin.server;

import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.server.mysql.packet.ErrorResponsePacket;
import com.wojustme.goblin.server.mysql.result.MysqlResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.SqlPlannerConf;
import com.wojustme.goblin.sql.ex.SqlParseException;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlDdl;
import io.netty.channel.ChannelHandlerContext;
import org.apache.calcite.sql.SqlNode;

/** One session handler for net request. */
public class SessionHandler {

  public final UserSession userSession;

  private SqlPlanner sqlPlanner;

  public SessionHandler(UserSession userSession, CatalogService catalogService) {
    this.userSession = userSession;
    final SqlPlannerConf sqlPlannerConf = new SqlPlannerConf(catalogService);
    this.sqlPlanner = new SqlPlanner(userSession, sqlPlannerConf);
  }


  public void exec(ChannelHandlerContext ctx, int sequenceId, String query) {
    try{
      final SqlNode parsed = sqlPlanner.parse(query);
      switch (parsed) {
        case GoblinSqlDdl ddlNode -> {
          ddlNode.exec(sqlPlanner.getCatalogService());
          final MysqlResult result = MysqlResult.create();
          result.flush(ctx, sequenceId);
        }
        default -> throw new SqlParseException("Not support sql: " + query);
      }
    } catch (Throwable t) {
      final ErrorResponsePacket errorPacket = ErrorResponsePacket.createOf(++sequenceId, 1, t.getMessage());
      ctx.writeAndFlush(errorPacket);
    }
  }
}
