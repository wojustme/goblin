package com.wojustme.goblin.sql.phys;

import com.wojustme.goblin.storage.impl.TableDesc;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.List;

/** Goblin's physical {@link TableScan} */
public class GPTableScan extends TableScan {

  private final List<String> requiredColumns = new ArrayList<>();

  private TableDesc tableDesc;

  public GPTableScan(
      RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }
}
