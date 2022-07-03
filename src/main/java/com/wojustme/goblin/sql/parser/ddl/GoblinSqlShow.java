package com.wojustme.goblin.sql.parser.ddl;

import com.wojustme.goblin.meta.catalog.CatalogService;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface GoblinSqlShow {

  SqlShow.ShowTag tag();

  Collection<String> show(CatalogService catalogService);
}
