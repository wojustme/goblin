package com.wojustme.goblin.sql.parser;

import com.wojustme.goblin.meta.catalog.CatalogService;

import java.util.Collection;

public interface GoblinSqlShow {

  SqlShow.ShowTag tag();

  Collection<String> show(CatalogService catalogService);
}
