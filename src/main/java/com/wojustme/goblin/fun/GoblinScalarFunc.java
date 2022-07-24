package com.wojustme.goblin.fun;

public interface GoblinScalarFunc extends GoblinFunc {
  default void setup() {}

  void eval();
}
