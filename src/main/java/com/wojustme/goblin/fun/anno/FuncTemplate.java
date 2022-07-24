package com.wojustme.goblin.fun.anno;

import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Template for building goblin's function. <br>
 * Notes: {@link FuncTemplate#name()} or {@link FuncTemplate#names()} must be setting value.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface FuncTemplate {

  /** Function's name */
  String name() default StringUtils.EMPTY;

  /** Function's aliases */
  String[] names() default {};

  NullHandling nulls() default NullHandling.NULL_IF_NULL;

  boolean iaVarArgs() default false;

  enum NullHandling {
    INTERNAL,
    NULL_IF_NULL,
    ;
  }
}
