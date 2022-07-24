package com.wojustme.goblin.sql.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.wojustme.goblin.meta.catalog.CatalogService;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public interface GoblinSqlDdl {

  void exec(CatalogService catalogService);

  default Pair<String, String> parseTableNamespace(
      CatalogService catalogService, SqlIdentifier tableIdentifier) {
    final ImmutableList<String> names = tableIdentifier.names;
    final int identifierNameSize = names.size();
    Preconditions.checkArgument(identifierNameSize <= 2 && identifierNameSize > 0);
    String dbName = catalogService.defaultDb();
    if (identifierNameSize == 2) {
      dbName = names.get(0);
    }
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(dbName), "Please make sure table of database.");
    final String tableName = names.get(identifierNameSize - 1);
    return Pair.of(dbName, tableName);
  }
}
