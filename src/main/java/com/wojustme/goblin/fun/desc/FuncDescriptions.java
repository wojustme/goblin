package com.wojustme.goblin.fun.desc;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.fun.FunctionRegistry;
import com.wojustme.goblin.fun.GoblinFunc;
import com.wojustme.goblin.fun.anno.FuncTemplate;
import com.wojustme.goblin.fun.anno.InputParam;
import com.wojustme.goblin.fun.anno.OutputData;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.reflections.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FuncDescriptions {

  public record InputParamDesc(Class<? extends ValueHolder> paramType, boolean constant) {}

  public record OutputDataDesc(Class<? extends ValueHolder> outType) {}

  public record ScalarFuncDesc(
      List<String> names,
      FuncTemplate.NullHandling nulls,
      boolean varArgs,
      List<InputParamDesc> inputs,
      OutputDataDesc out) {
    public static ScalarFuncDesc analyze(Class<GoblinFunc> funcClass) {
      final List<String> names = analyzeFuncNames(funcClass);
      final Pair<List<InputParamDesc>, List<OutputDataDesc>> ioPair = extractFuncIO(funcClass);
      Preconditions.checkArgument(ioPair.getRight().size() == 1);
      final FuncTemplate funcTemplate = funcClass.getAnnotation(FuncTemplate.class);
      return new ScalarFuncDesc(names, funcTemplate.nulls(), funcTemplate.iaVarArgs(), ioPair.getLeft(), ioPair.getRight().get(0));
    }
  }




  private static List<String> analyzeFuncNames(Class funcClass) {
    Preconditions.checkArgument(funcClass.isAnnotationPresent(FuncTemplate.class));
    final FuncTemplate funcTemplate = (FuncTemplate) funcClass.getAnnotation(FuncTemplate.class);
    final String classname = ClassUtils.getName(funcClass);
    final List<String> names = new ArrayList<>();
    if (StringUtils.isNotEmpty(funcTemplate.name())) {
      names.add(funcTemplate.name());
    }
    if (ArrayUtils.isNotEmpty(funcTemplate.names())) {
      names.addAll(Arrays.asList(funcTemplate.names()));
    }
    Preconditions.checkArgument(
            CollectionUtils.isNotEmpty(names), "%s not set name or names", classname);
    return names;
  }

  private static Pair<List<InputParamDesc>, List<OutputDataDesc>> extractFuncIO(Class<GoblinFunc> funcClass) {
    final List<InputParamDesc> inputs = new ArrayList<>();
    final List<OutputDataDesc> outputs = new ArrayList<>();
    for (Field field : ReflectionUtils.getAllFields(funcClass)) {
      if (field.isAnnotationPresent(InputParam.class)) {
        final InputParam inputParam = field.getAnnotation(InputParam.class);
        inputs.add(new InputParamDesc((Class<ValueHolder>)field.getType(), inputParam.constant()));
      } else if (field.isAnnotationPresent(OutputData.class)) {
        outputs.add(new OutputDataDesc((Class<ValueHolder>)field.getType()));
      }
    }
    return Pair.of(inputs,outputs);
  }
}
