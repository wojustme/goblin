package com.wojustme.goblin.sql;

import com.google.common.collect.Lists;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.sql.schema.MetaIntegrates;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.List;
import java.util.Properties;

/** Conf for {@link SqlPlanner} */
public class SqlPlannerConf {

  private final SqlDialect dialect;

  public final SqlParser.Config parserConfig;

  public final RelDataTypeFactory typeFactory;

  public final CatalogService catalogService;
  public final Prepare.CatalogReader catalogReader;

  public final SqlOperatorTable operatorTable;

  public final SqlValidator validator;

  public SqlPlannerConf(CatalogService catalogService) {
    this.dialect = MysqlSqlDialect.DEFAULT;
    this.parserConfig = dialect.configureParser(SqlParser.Config.DEFAULT);
    this.typeFactory = new JavaTypeFactoryImpl(dialect.getTypeSystem());
    this.catalogService = catalogService;
    CalciteConnectionConfigImpl connConfig = new CalciteConnectionConfigImpl(new Properties());
    this.catalogReader = CatalogReader.getCatalogReader(catalogService, typeFactory, connConfig);
    this.operatorTable = SqlOperatorTables.chain();
    this.validator = new MySqlValidator(operatorTable, catalogReader, typeFactory);
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

    private static CatalogReader getCatalogReader(
        CatalogService catalogService,
        RelDataTypeFactory typeFactory,
        CalciteConnectionConfigImpl connConfig) {

      // define root schema
      final MetaIntegrates.MySchema rootSchema =
          new MetaIntegrates.MySchema(catalogService, typeFactory, null);
      final MetaIntegrates.MySchemaWrapper rootSchemaWrapper =
          new MetaIntegrates.MySchemaWrapper(null, rootSchema, "");

      // define default schema
      final String defaultSchemaName = catalogService.defaultDb();
      final MetaIntegrates.MySchema defaultSchema =
          new MetaIntegrates.MySchema(catalogService, typeFactory, defaultSchemaName);
      final MetaIntegrates.MySchemaWrapper defaultSchemaWrapper =
          new MetaIntegrates.MySchemaWrapper(
              rootSchemaWrapper, defaultSchema, defaultSchemaName);

      return new CatalogReader(rootSchemaWrapper, defaultSchemaWrapper, typeFactory, connConfig);
    }
  }

  static class MySqlValidator extends SqlValidatorImpl {
    private MySqlValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory) {
      super(opTab, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
    }
  }
}
