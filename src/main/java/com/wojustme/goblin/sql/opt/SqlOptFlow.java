package com.wojustme.goblin.sql.opt;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;

public class SqlOptFlow {
  public RelNode run(RelNode relNode) {
    final HepProgramBuilder hepBuilder = new HepProgramBuilder();
    RelOptRules.BASE_RBO_RULES.forEach(hepBuilder::addRuleInstance);
    final HepProgram hepProgram = hepBuilder.build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(relNode);
    return hepPlanner.findBestExp();
  }
}
