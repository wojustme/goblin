package com.wojustme.goblin.sql.opt;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.List;

public class RelOptRules {

  /** Some based rules, which is based on rule-based-optimizer */
  public static final List<RelOptRule> BASE_RBO_RULES =
      ImmutableList.of(CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
}
