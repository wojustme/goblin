package com.wojustme.goblin.fun;

import com.wojustme.goblin.fun.anno.FuncTemplate;
import com.wojustme.goblin.fun.calcite.GoblinSqlUserDefinedFunc;
import com.wojustme.goblin.fun.desc.FuncDescriptions;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.reflections.Reflections;

import java.util.Set;

public class FunctionRegistry {

  private final String scanPackage;

  public FunctionRegistry(String scanPackage) {
    this.scanPackage = scanPackage;
  }

  public void register(ReflectiveSqlOperatorTable operatorTable) {
    // 1. scan package's path
    final Reflections reflections = new Reflections(scanPackage);

    final Set<Class<?>> funcClasses = reflections.getTypesAnnotatedWith(FuncTemplate.class);

    for (Class funcClass : funcClasses) {
      if (GoblinScalarFunc.class.isAssignableFrom(funcClass)) {
        registerUDF(operatorTable, funcClass);
      }
    }
    // 2. create operator for calcite
    // 3. register into calcite
  }

  private void registerUDF(ReflectiveSqlOperatorTable operatorTable, Class<GoblinFunc> funcClass) {
    final FuncDescriptions.ScalarFuncDesc scalarFuncDesc =
        FuncDescriptions.ScalarFuncDesc.analyze(funcClass);
    for (String name : scalarFuncDesc.names()) {
      operatorTable.register(new GoblinSqlUserDefinedFunc(name, scalarFuncDesc));
    }
  }
}
