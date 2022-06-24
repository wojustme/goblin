package com.wojustme.goblin.sql;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.Lists;
import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Properties;

/** Conf for {@link SqlPlanner} */
public class SqlPlannerConf {

  private final SqlDialect dialect;

  public final SqlParser.Config parserConfig;

  public final RelDataTypeFactory typeFactory;

  public final CatalogService catalogService;
  public final Prepare.CatalogReader catalogReader;

  public SqlPlannerConf(CatalogService catalogService) {
    this.dialect = MysqlSqlDialect.DEFAULT;
    this.parserConfig = dialect.configureParser(SqlParser.Config.DEFAULT);
    this.typeFactory = new JavaTypeFactoryImpl(dialect.getTypeSystem());
    this.catalogService = catalogService;
    CalciteConnectionConfigImpl connConfig = new CalciteConnectionConfigImpl(new Properties());
    catalogReader = null;
    //        new CatalogReader(catalogService, catalogService.defaultDb(), typeFactory,
    // connConfig);
  }


  public static class CatalogReader extends CalciteCatalogReader {
    public final String defaultDb;

    private CatalogReader(
        CalciteSchema rootSchema,
        CalciteSchema defaultSchema,
        RelDataTypeFactory typeFactory,
        CalciteConnectionConfig calciteConnectionConfig) {
      super(
          rootSchema, Lists.newArrayList(defaultSchema.name), typeFactory, calciteConnectionConfig);
      this.defaultDb = defaultSchema.name;
    }
  }
}
