package com.wojustme.goblin.sql.phys;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexProgram;

import java.util.List;

/** Goblin's physical {@link Calc} */
public class GPCalc extends Calc {
  public GPCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, hints, child, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
    return null;
  }
}
