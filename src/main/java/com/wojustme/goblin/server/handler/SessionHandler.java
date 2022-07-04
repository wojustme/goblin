package com.wojustme.goblin.server.handler;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.common.GoblinContext;
import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.server.handler.result.FailedResult;
import com.wojustme.goblin.server.handler.result.HandlerResult;
import com.wojustme.goblin.sql.SqlPlanner;
import com.wojustme.goblin.sql.SqlPlannerConf;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlDdl;
import com.wojustme.goblin.sql.parser.ddl.GoblinSqlShow;
import com.wojustme.goblin.storage.impl.StorageCatalogService;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One session handler for net request. */
public class SessionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionHandler.class);

  public final GoblinContext goblinContext;
  public final UserSession userSession;

  public final CatalogService catalogService;

  public SessionHandler(GoblinContext goblinContext, UserSession userSession) {
    this.goblinContext = goblinContext;
    this.userSession = userSession;
    this.catalogService = new StorageCatalogService(StringUtils.EMPTY, goblinContext.storageManager);
  }

  public HandlerResult exec(String query) {
    final SqlPlannerConf sqlPlannerConf = new SqlPlannerConf(catalogService);
    final SqlPlanner sqlPlanner = new SqlPlanner(userSession, sqlPlannerConf);
    try {
      final SqlNode parsed = sqlPlanner.parse(query);
      Preconditions.checkArgument(parsed != null  && !(parsed instanceof SqlNodeList ), "Goblin only support single statement");
      return execEachNode(sqlPlanner, parsed);
    } catch (Throwable t) {
      LOGGER.error("Session:{} exec sql fail, cause by:", userSession.sessionId, t);
      return new FailedResult(404, t.getMessage());
    }
  }

  public HandlerResult execEachNode(SqlPlanner sqlPlanner, SqlNode sqlNode) {
    AbstractSqlHandler sqlHandler = switch (sqlNode) {
      case GoblinSqlDdl ddlNode -> new DDLSqlHandler(sqlNode);
      case GoblinSqlShow showNode -> new ShowSqlHandler(sqlNode);
      default -> new DefaultSqlHandler(sqlNode);
    };
    return sqlHandler.exec(sqlPlanner);
  }
}
