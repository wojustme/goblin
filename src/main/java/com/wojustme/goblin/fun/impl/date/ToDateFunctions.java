package com.wojustme.goblin.fun.impl.date;

import com.wojustme.goblin.fun.GoblinScalarFunc;
import com.wojustme.goblin.fun.anno.FuncTemplate;
import com.wojustme.goblin.fun.anno.InputParam;
import com.wojustme.goblin.fun.anno.OutputData;
import org.apache.arrow.vector.holders.DateDayHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

public class ToDateFunctions {

  @FuncTemplate(name = "to_date")
  public static class VarcharToDateFunc implements GoblinScalarFunc {

    @InputParam private VarCharHolder dateStr;

    @InputParam(constant = true)
    private VarCharHolder format;


    @OutputData
    private DateDayHolder out;

    @Override
    public void eval() {
    }
  }
}
