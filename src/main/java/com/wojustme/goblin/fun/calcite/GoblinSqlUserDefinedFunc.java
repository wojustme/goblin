package com.wojustme.goblin.fun.calcite;

import com.google.common.base.Preconditions;
import com.wojustme.goblin.fun.desc.FuncDescriptions;
import com.wojustme.goblin.sql.util.SqlUtils;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GoblinSqlUserDefinedFunc extends SqlFunction {

  private final FuncDescriptions.ScalarFuncDesc scalarFuncDesc;

  public GoblinSqlUserDefinedFunc(String name, FuncDescriptions.ScalarFuncDesc scalarFuncDesc) {
    super(
        name, SqlKind.OTHER_FUNCTION, null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.scalarFuncDesc = scalarFuncDesc;
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    return getOperandTypeChecker().checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final Class<? extends ValueHolder> holderClass = scalarFuncDesc.out().outType();
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType relDataType = TypeConverter.convertType(typeFactory, holderClass);
    return relDataType;
  }

  @Override
  public @Nullable SqlOperandTypeChecker getOperandTypeChecker() {
    return new SqlOperandTypeChecker() {
      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        final List<FuncDescriptions.InputParamDesc> inputs = scalarFuncDesc.inputs();
        final List<SqlNode> operands = callBinding.operands();
        Preconditions.checkArgument(
            inputs.size() == operands.size(), "Validate %s error", callBinding.getCall());
        for (int i = 0; i < operands.size(); i++) {
          final SqlNode operand = operands.get(i);
          final FuncDescriptions.InputParamDesc inputParamDesc = inputs.get(i);
          if (inputParamDesc.constant()) {
            Preconditions.checkArgument(
                operand instanceof SqlLiteral,
                "Index: %s operand of %s require constant node",
                i,
                callBinding.getCall());
          }
          final RelDataType operandType = callBinding.getOperandType(i);
          final RelDataType funcType =
              TypeConverter.convertType(typeFactory, inputParamDesc.paramType());
          final RelDataTypeFamily family = funcType.getFamily();
          if (family instanceof SqlTypeFamily sqlTypeFamily) {
            // Here add some hint to allow cast implicit.
            if (sqlTypeFamily.contains(operandType)) {
              continue;
            }
          }
          Preconditions.checkArgument(
              SqlUtils.equalRelType(funcType, operandType),
              "Function: %s validate error, require %s, but it's %s",
              callBinding.getCall(),
              funcType,
              operandType);
        }
        return true;
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return GoblinSqlUserDefinedFunc.this.getOperandCountRange();
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return null;
      }

      @Override
      public Consistency getConsistency() {
        return null;
      }

      @Override
      public boolean isOptional(int i) {
        return false;
      }
    };
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    if (scalarFuncDesc.varArgs()) {
      return SqlOperandCountRanges.any();
    } else {
      return SqlOperandCountRanges.of(scalarFuncDesc.inputs().size());
    }
  }
}
