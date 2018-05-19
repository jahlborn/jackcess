/*
Copyright (c) 2016 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.impl.expr;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.FunctionLookup;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFunctions
{
  private static final Map<String,Function> FUNCS =
    new HashMap<String,Function>();

  private static final char NON_VAR_SUFFIX = '$';

  static {
    // load all default functions
    DefaultTextFunctions.init();
    DefaultNumberFunctions.init();
    DefaultDateFunctions.init();
    DefaultFinancialFunctions.init();
  }

  public static final FunctionLookup LOOKUP = new FunctionLookup() {
    public Function getFunction(String name) {
      return DefaultFunctions.getFunction(name);
    }
  };

  private DefaultFunctions() {}

  public static Function getFunction(String name) {
    return FUNCS.get(DatabaseImpl.toLookupName(name));
  }

  public static abstract class BaseFunction implements Function
  {
    private final String _name;
    private final int _minParams;
    private final int _maxParams;

    protected BaseFunction(String name, int minParams, int maxParams)
    {
      _name = name;
      _minParams = minParams;
      _maxParams = maxParams;
    }

    public String getName() {
      return _name;
    }

    public boolean isPure() {
      // most functions are probably pure, so make this the default
      return true;
    }

    protected void validateNumParams(Value[] params) {
      int num = params.length;
      if((num < _minParams) || (num > _maxParams)) {
        String range = ((_minParams == _maxParams) ? "" + _minParams :
                        _minParams + " to " + _maxParams);
        throw new EvalException(
            "Invalid number of parameters " +
            num + " passed, expected " + range);
      }
    }

    protected IllegalStateException invalidFunctionCall(
        Throwable t, Value[] params)
    {
      String paramStr = Arrays.toString(params);
      String msg = "Invalid function call {" + _name + "(" +
        paramStr.substring(1, paramStr.length() - 1) + ")}";
      return new IllegalStateException(msg, t);
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }

  public static abstract class Func0 extends BaseFunction
  {
    protected Func0(String name) {
      super(name, 0, 0);
    }

    @Override
    public boolean isPure() {
      // 0-arg functions are usually not pure
      return false;
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval0(ctx);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval0(EvalContext ctx);
  }

  public static abstract class Func1 extends BaseFunction
  {
    protected Func1(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval1(ctx, params[0]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func1NullIsNull extends BaseFunction
  {
    protected Func1NullIsNull(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        Value param1 = params[0];
        if(param1.isNull()) {
          return param1;
        }
        return eval1(ctx, param1);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func2 extends BaseFunction
  {
    protected Func2(String name) {
      super(name, 2, 2);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval2(ctx, params[0], params[1]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval2(EvalContext ctx, Value param1, Value param2);
  }

  public static abstract class Func3 extends BaseFunction
  {
    protected Func3(String name) {
      super(name, 3, 3);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval3(ctx, params[0], params[1], params[2]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval3(EvalContext ctx,
                                   Value param1, Value param2, Value param3);
  }

  public static abstract class FuncVar extends BaseFunction
  {
    protected FuncVar(String name) {
      super(name, 0, Integer.MAX_VALUE);
    }

    protected FuncVar(String name, int minParams, int maxParams) {
      super(name, minParams, maxParams);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return evalVar(ctx, params);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value evalVar(EvalContext ctx, Value[] params);
  }

  public static class StringFuncWrapper implements Function
  {
    private final String _name;
    private final Function _delegate;

    public StringFuncWrapper(Function delegate) {
      _delegate = delegate;
      _name = _delegate.getName() + NON_VAR_SUFFIX;
    }

    public String getName() {
      return _name;
    }

    public boolean isPure() {
      return _delegate.isPure();
    }

    public Value eval(EvalContext ctx, Value... params) {
      Value result = _delegate.eval(ctx, params);
      if(result.isNull()) {
        // non-variant version does not do null-propagation, so force
        // exception to be thrown here
        result.getAsString();
      }
      return result;
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }


  public static final Function IIF = registerFunc(new Func3("IIf") {
    @Override
    protected Value eval3(EvalContext ctx,
                          Value param1, Value param2, Value param3) {
      // null is false
      return ((!param1.isNull() && param1.getAsBoolean()) ? param2 : param3);
    }
  });

  public static final Function HEX = registerStringFunc(new Func1NullIsNull("Hex") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      if((param1.getType() == Value.Type.STRING) &&
         (param1.getAsString().length() == 0)) {
        return BuiltinOperators.ZERO_VAL;
      }
      int lv = param1.getAsLongInt();
      return BuiltinOperators.toValue(Integer.toHexString(lv).toUpperCase());
    }
  });

  public static final Function NZ = registerFunc(new FuncVar("Nz", 1, 2) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(!param1.isNull()) {
        return param1;
      }
      if(params.length > 1) {
        return params[1];
      }
      Value.Type resultType = ctx.getResultType();
      return (((resultType == null) ||
               (resultType == Value.Type.STRING)) ?
              BuiltinOperators.EMPTY_STR_VAL : BuiltinOperators.ZERO_VAL);
    }
  });

  public static final Function CHOOSE = registerFunc(new FuncVar("Choose", 1, Integer.MAX_VALUE) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      int idx = param1.getAsLongInt();
      if((idx < 1) || (idx >= params.length)) {
        return BuiltinOperators.NULL_VAL;
      }
      return params[idx];
    }
  });

  public static final Function SWITCH = registerFunc(new FuncVar("Switch") {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      if((params.length % 2) != 0) {
        throw new EvalException("Odd number of parameters");
      }
      for(int i = 0; i < params.length; i+=2) {
        if(params[i].getAsBoolean()) {
          return params[i + 1];
        }
      }
      return BuiltinOperators.NULL_VAL;
    }
  });

  public static final Function OCT = registerStringFunc(new Func1NullIsNull("Oct") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      if((param1.getType() == Value.Type.STRING) &&
         (param1.getAsString().length() == 0)) {
        return BuiltinOperators.ZERO_VAL;
      }
      int lv = param1.getAsLongInt();
      return BuiltinOperators.toValue(Integer.toOctalString(lv));
    }
  });

  public static final Function CBOOL = registerFunc(new Func1("CBool") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      boolean b = param1.getAsBoolean();
      return BuiltinOperators.toValue(b);
    }
  });

  public static final Function CBYTE = registerFunc(new Func1("CByte") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      if((lv < 0) || (lv > 255)) {
        throw new EvalException("Byte code '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CCUR = registerFunc(new Func1("CCur") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal();
      bd = bd.setScale(4, BuiltinOperators.ROUND_MODE);
      return BuiltinOperators.toValue(bd);
    }
  });

  public static final Function CDATE = registerFunc(new Func1("CDate") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return DefaultDateFunctions.nonNullToDateValue(ctx, param1);
    }
  });
  static {
    registerFunc("CVDate", CDATE);
  }

  public static final Function CDBL = registerFunc(new Func1("CDbl") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Double dv = param1.getAsDouble();
      return BuiltinOperators.toValue(dv);
    }
  });

  public static final Function CDEC = registerFunc(new Func1("CDec") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal();
      return BuiltinOperators.toValue(bd);
    }
  });

  public static final Function CINT = registerFunc(new Func1("CInt") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      if((lv < Short.MIN_VALUE) || (lv > Short.MAX_VALUE)) {
        throw new EvalException("Int value '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CLNG = registerFunc(new Func1("CLng") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CSNG = registerFunc(new Func1("CSng") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Double dv = param1.getAsDouble();
      if((dv < Float.MIN_VALUE) || (dv > Float.MAX_VALUE)) {
        throw new EvalException("Single value '" + dv + "' out of range ");
      }
      return BuiltinOperators.toValue(dv.floatValue());
    }
  });

  public static final Function CSTR = registerFunc(new Func1("CStr") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(param1.getAsString());
    }
  });

  public static final Function CVAR = registerFunc(new Func1("CVar") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return param1;
    }
  });

  public static final Function ISNULL = registerFunc(new Func1("IsNull") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(param1.isNull());
    }
  });

  public static final Function ISDATE = registerFunc(new Func1("IsDate") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(
          !param1.isNull() &&
          (DefaultDateFunctions.nonNullToDateValue(ctx, param1) != null));
    }
  });

  public static final Function VARTYPE = registerFunc(new Func1("VarType") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value.Type type = param1.getType();
      int vType = 0;
      switch(type) {
      case NULL:
        // vbNull
        vType = 1;
        break;
      case STRING:
        // vbString
        vType = 8;
        break;
      case DATE:
      case TIME:
      case DATE_TIME:
        // vbDate
        vType = 7;
        break;
      case LONG:
        // vbLong
        vType = 3;
        break;
      case DOUBLE:
        // vbDouble
        vType = 5;
        break;
      case BIG_DEC:
        // vbDecimal
        vType = 14;
        break;
      default:
        throw new EvalException("Unknown type " + type);
      }
      return BuiltinOperators.toValue(vType);
    }
  });

  public static final Function TYPENAME = registerFunc(new Func1("TypeName") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Value.Type type = param1.getType();
      String tName = null;
      switch(type) {
      case NULL:
        tName = "Null";
        break;
      case STRING:
        tName = "String";
        break;
      case DATE:
      case TIME:
      case DATE_TIME:
        tName = "Date";
        break;
      case LONG:
        tName = "Long";
        break;
      case DOUBLE:
        tName = "Double";
        break;
      case BIG_DEC:
        tName = "Decimal";
        break;
      default:
        throw new EvalException("Unknown type " + type);
      }
      return BuiltinOperators.toValue(tName);
    }
  });



  // https://www.techonthenet.com/access/functions/
  // https://support.office.com/en-us/article/Access-Functions-by-category-b8b136c3-2716-4d39-94a2-658ce330ed83

  static Function registerFunc(Function func) {
    registerFunc(func.getName(), func);
    return func;
  }

  static Function registerStringFunc(Function func) {
    registerFunc(func.getName(), func);
    registerFunc(new StringFuncWrapper(func));
    return func;
  }

  private static void registerFunc(String fname, Function func) {
    String lookupFname = DatabaseImpl.toLookupName(fname);
    if(FUNCS.put(lookupFname, func) != null) {
      throw new IllegalStateException("Duplicate function " + fname);
    }
  }
}
