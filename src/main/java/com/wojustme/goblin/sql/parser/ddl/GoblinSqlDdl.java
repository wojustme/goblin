package com.wojustme.goblin.sql.parser.ddl;

import com.wojustme.goblin.meta.catalog.CatalogService;

public interface GoblinSqlDdl {

  void exec(CatalogService catalogService);
}
