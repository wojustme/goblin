package com.wojustme.goblin.sql;

import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;

/** Sql planner */
public class SqlPlanner {

  private final UserSession userSession;

  private final SqlPlannerConf conf;

  public SqlPlanner(UserSession userSession, SqlPlannerConf conf) {
    this.userSession = userSession;
    this.conf = conf;
  }

  public CatalogService getCatalogService() {
    return conf.catalogService;
  }

  public SqlNode parse(String sql) {
    final SqlParser parser = SqlParser.create(sql, conf.parserConfig);
    try {
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new com.wojustme.goblin.sql.ex.SqlParseException(e, "Parsing sql error:\n%s", sql);
    }
  }

  public SqlNode validate(SqlNode sqlNode) {
    return null;
  }

  public RelNode convertRel(SqlNode validatedNode) {
    return null;
  }

  public RelNode optimize(RelNode relNode, List<RelOptRule> rules) {
    return null;
  }
}
