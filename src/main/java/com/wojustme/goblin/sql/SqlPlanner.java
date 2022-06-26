package com.wojustme.goblin.sql;

import com.wojustme.goblin.common.UserSession;
import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;

/** Sql planner */
public class SqlPlanner {

  private final UserSession userSession;

  private final SqlPlannerConf plannerConf;

  public SqlPlanner(UserSession userSession, SqlPlannerConf plannerConf) {
    this.userSession = userSession;
    this.plannerConf = plannerConf;
  }

  public CatalogService getCatalogService() {
    return plannerConf.catalogService;
  }

  public SqlNode parse(String sql) {
    // TMP code to rewrite sql by regular, because of calcite-parser's defects.
    // eg: 'select @@version' => `select `@@version`'.
    final String rewritedSql = rewriteSql(sql);
    final SqlParser parser = SqlParser.create(rewritedSql, plannerConf.parserConfig);
    try {
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new com.wojustme.goblin.sql.ex.SqlParseException(e, "Parsing sql error:\n%s", sql);
    }
  }

  public SqlNode validate(SqlNode sqlNode) {
    return plannerConf.validator.validate(sqlNode);
  }

  public RelNode convertRel(SqlNode validatedNode) {
    final RelRoot relRoot = convertRelRoot(validatedNode);
    return relRoot.rel;
  }

  private RelRoot convertRelRoot(SqlNode validatedNode) {
    final SqlToRelConverter.Config config = SqlToRelConverter.config();
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(
            new MyViewExpander(),
            plannerConf.validator,
            plannerConf.catalogReader,
            buildRelCluster(),
            StandardConvertletTable.INSTANCE,
            config);
    return sqlToRelConverter.convertQuery(validatedNode, false, true);
  }

  public RelNode optimize(RelNode relNode, List<RelOptRule> rules) {
    return null;
  }

  // Some private util-methods
  private static String rewriteSql(String sql) {
    return sql.replaceAll("@{1,2}\\w+", "'$0'");
  }

  private RelOptCluster buildRelCluster() {
    final RelOptCluster relOptCluster =
        RelOptCluster.create(buildHepPlanner(), new RexBuilder(plannerConf.typeFactory));
    relOptCluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);
    return relOptCluster;
  }

  private HepPlanner buildHepPlanner() {
    final HepProgramBuilder builder = new HepProgramBuilder();
    builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    builder.addMatchLimit(10);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.setExecutor(new RexExecutorImpl(null));
    return hepPlanner;
  }

  private class MyViewExpander implements RelOptTable.ViewExpander {
    @Override
    public RelRoot expandView(
        RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
      final SqlPlanner currentPlanner = SqlPlanner.this;
      final SqlNode parseNode = currentPlanner.parse(queryString);
      final SqlNode validatedNode = currentPlanner.validate(parseNode);
      return currentPlanner.convertRelRoot(validatedNode);
    }
  }
}
