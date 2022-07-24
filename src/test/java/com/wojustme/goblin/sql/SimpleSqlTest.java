package com.wojustme.goblin.sql;

import com.wojustme.goblin.meta.catalog.model.CatalogTable;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SimpleSqlTest extends GoblinSqlBaseTest {

  @Test
  public void testFlow() {
    // create table t
    execDDL("create table t(id varchar)");
    // assert table exist
    final CatalogTable catalogTable = catalogService.getTable(DEFAULT_DB, "t");
    Assert.assertNotNull(catalogTable);
    // drop table t
    execDDL("drop table t");
    // assert table not exist
    Assert.assertNull(catalogService.getTable(DEFAULT_DB, "t"));
  }
}
