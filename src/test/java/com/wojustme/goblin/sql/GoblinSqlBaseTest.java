package com.wojustme.goblin.sql;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.GoblinBaseTest;
import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.fun.FunctionRegistry;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.impl.InMemoryCatalogService;
import com.wojustme.goblin.sql.parser.GoblinSqlDdl;
import org.apache.calcite.sql.SqlNode;

public class GoblinSqlBaseTest extends GoblinBaseTest {

  protected String DEFAULT_DB = "default";
  protected SqlPlanner sqlPlanner;

  protected CatalogService catalogService;

  protected UserSession userSession;

  public GoblinSqlBaseTest() {
    this.userSession = new UserSession("session01", "admin");
    userSession.setDatabase(DEFAULT_DB);
    this.catalogService = new InMemoryCatalogService(DEFAULT_DB);
    final SqlPlannerConf sqlPlannerConf = new SqlPlannerConf(catalogService, new FunctionRegistry("com.wojustme.goblin.fun.impl"));
    this.sqlPlanner = new SqlPlanner(userSession, sqlPlannerConf);
  }

  protected void execDDL(String ddl) {
    final SqlNode parse = sqlPlanner.parse(ddl);
    Preconditions.checkArgument(parse instanceof GoblinSqlDdl);
    ((GoblinSqlDdl) parse).exec(catalogService);
  }
}
