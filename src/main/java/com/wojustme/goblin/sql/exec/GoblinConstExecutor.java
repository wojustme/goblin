package com.wojustme.goblin.sql.exec;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class GoblinConstExecutor implements RexExecutor {

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {}
}
