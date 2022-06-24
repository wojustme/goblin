package com.wojustme.goblin.sql.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.wojustme.goblin.meta.catalog.CatalogService;
import com.wojustme.goblin.meta.catalog.model.CatalogDatabase;
import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Set;

public final class MetaIntegrates {

  public static class MySchemaWrapper extends CalciteSchema {

    private final CalciteSchema parent;

    public MySchemaWrapper(CalciteSchema parent, Schema currentSchema, String name) {
      super(parent, currentSchema, name, null, null, null, null, null, null, null, null);
      this.parent = parent;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(
        String schemaName, boolean caseSensitive) {
      final Schema subSchema = schema.getSubSchema(schemaName);
      return subSchema == null ? null : new MySchemaWrapper(this.parent, subSchema, schemaName);
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
      final Table table = schema.getTable(tableName);
      return table == null ? null : tableEntry(tableName, table);
    }

    @Override
    protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
      return null;
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(
        String tableName, boolean caseSensitive) {
      return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(
        ImmutableSortedMap.Builder<String, CalciteSchema> builder) {}

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {}

    @Override
    protected void addImplicitFunctionsToBuilder(
        ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {}

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {}

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {}

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
        ImmutableSortedMap.Builder<String, Table> builder) {}

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
      return null;
    }

    @Override
    protected boolean isCacheEnabled() {
      return false;
    }

    @Override
    public void setCache(boolean cache) {}

    @Override
    public CalciteSchema add(String name, Schema schema) {
      return null;
    }
  }

  public static class MySchema implements Schema {
    /** catalog service's instance. */
    private final CatalogService catalog;
    /** schema's name. */
    private final String dbName;
    /** current type factory. */
    private final RelDataTypeFactory typeFactory;

    public MySchema(CatalogService catalog, RelDataTypeFactory typeFactory, String dbName) {
      this.catalog = catalog;
      this.typeFactory = typeFactory;
      this.dbName = dbName;
    }

    @Override
    public @Nullable Table getTable(String name) {
      final CatalogTable catalogTable = catalog.getTable(dbName, name);
      return catalogTable == null ? null : new CalciteTable(typeFactory, catalogTable);
    }

    @Override
    public Set<String> getTableNames() {
      return null;
    }

    @Override
    public @Nullable RelProtoDataType getType(String name) {
      return null;
    }

    @Override
    public Set<String> getTypeNames() {
      return null;
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return null;
    }

    @Override
    public Set<String> getFunctionNames() {
      return null;
    }

    @Override
    public @Nullable Schema getSubSchema(String name) {
      /* If dbName is null, this schema is root schema.*/
      if (dbName == null) {
        final CatalogDatabase database = catalog.getDatabase(name);
        return database == null ? null : new MySchema(catalog, typeFactory, name);
      } else {
        // If defaultDb == name, we should return null for avoiding `default`.`default`.
        if (dbName.equals(name)) {
          return null;
        } else {
          final CatalogDatabase database = catalog.getDatabase(name);
          return database == null ? null : new MySchema(catalog, typeFactory, name);
        }
      }
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return null;
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
      return null;
    }

    @Override
    public boolean isMutable() {
      return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
      return null;
    }
  }
}
