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
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFunctions 
{
  private static final Map<String,Function> FUNCS = 
    new HashMap<String,Function>();

  private static final char NON_VAR_SUFFIX = '$';

  private DefaultFunctions() {}

  public static Function getFunction(String name) {
    return FUNCS.get(name.toLowerCase());
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
        throw new IllegalArgumentException(
            this + ": invalid number of parameters " +
            num + " passed, expected " + range);
      }
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }

  public static abstract class Func0 extends BaseFunction
  {
    protected Func0(String name) {
      super(name, 0, 9);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval0(ctx);
    }

    protected abstract Value eval0(EvalContext ctx);
  }

  public static abstract class Func1 extends BaseFunction
  {
    protected Func1(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval1(ctx, params[0]);
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func2 extends BaseFunction
  {
    protected Func2(String name) {
      super(name, 2, 2);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval2(ctx, params[0], params[1]);
    }

    protected abstract Value eval2(EvalContext ctx, Value param1, Value param2);
  }

  public static abstract class Func3 extends BaseFunction
  {
    protected Func3(String name) {
      super(name, 3, 3);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval3(ctx, params[0], params[1], params[2]);
    }

    protected abstract Value eval3(EvalContext ctx, 
                                   Value param1, Value param2, Value param3);
  }

  public static final Function IIF = registerFunc(new Func3("IIf") {
    @Override
    protected Value eval3(EvalContext ctx, 
                          Value param1, Value param2, Value param3) {
      // null is false
      return ((!param1.isNull() && param1.getAsBoolean()) ? param2 : param3);
    }
  });

  public static final Function ASC = registerFunc(new Func1("Asc") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      int len = str.length();
      if(len == 0) {
        throw new IllegalStateException("No characters in string");
      } 
      long lv = str.charAt(0);
      if((lv < 0) || (lv > 255)) {
        throw new IllegalStateException("Character code '" + lv +
                                        "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function ASCW = registerFunc(new Func1("AscW") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      int len = str.length();
      if(len == 0) {
        throw new IllegalStateException("No characters in string");
      } 
      long lv = str.charAt(0);
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CHR = registerStringFunc(new Func1("Chr") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      long lv = param1.getAsLong();
      if((lv < 0) || (lv > 255)) {
        throw new IllegalStateException("Character code '" + lv +
                                        "' out of range ");
      }
      char[] cs = Character.toChars((int)lv);
      return BuiltinOperators.toValue(new String(cs));
    }
  });

  public static final Function CHRW = registerStringFunc(new Func1("ChrW") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      long lv = param1.getAsLong();
      char[] cs = Character.toChars((int)lv);
      return BuiltinOperators.toValue(new String(cs));
    }
  });

  public static final Function HEX = registerStringFunc(new Func1("Hex") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      if(param1.isNull()) {
        return param1;
      }
      if((param1.getType() == Value.Type.STRING) &&
         (param1.getAsString().length() == 0)) {
        return BuiltinOperators.ZERO_VAL;
      }
      long lv = param1.getAsLong();
      return BuiltinOperators.toValue(Long.toHexString(lv));
    }
  });

  public static final Function NZ = registerFunc(new BaseFunction("Nz", 1, 2) {
    public Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
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

  public static final Function OCT = registerStringFunc(new Func1("Oct") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      if(param1.isNull()) {
        return param1;
      }
      if((param1.getType() == Value.Type.STRING) &&
         (param1.getAsString().length() == 0)) {
        return BuiltinOperators.ZERO_VAL;
      }
      long lv = param1.getAsLong();
      return BuiltinOperators.toValue(Long.toOctalString(lv));
    }
  });
  
  public static final Function STR = registerStringFunc(new Func1("Str") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal();
      String str = bd.toPlainString();
      if(bd.compareTo(BigDecimal.ZERO) >= 0) {
        str = " " + str;
      }
      return BuiltinOperators.toValue(str);
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
      Long lv = param1.getAsLong();
      if((lv < 0) || (lv > 255)) {
        throw new IllegalStateException("Byte code '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CCUR = registerFunc(new Func1("CCur") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal();
      bd.setScale(4, RoundingMode.HALF_EVEN);
      return BuiltinOperators.toValue(bd);
    }
  });

  // public static final Function CDATE = registerFunc(new Func1("CDate") {
  //   @Override
  //   protected Value eval1(EvalContext ctx, Value param1) {
  //   FIXME
  //     BigDecimal bd = param1.getAsBigDecimal();
  //     bd.setScale(4, RoundingMode.HALF_EVEN);
  //     return BuiltinOperators.toValue(bd);
  //   }
  // });

  public static final Function CDBL = registerFunc(new Func1("CDbl") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Double db = param1.getAsDouble();
      return BuiltinOperators.toValue(db);
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
      // FIXME, fix rounding for this and clng
      Long lv = param1.getAsLong();
      if((lv < Short.MIN_VALUE) || (lv > Short.MAX_VALUE)) {
        throw new IllegalStateException("Int value '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CLNG = registerFunc(new Func1("CLng") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Long lv = param1.getAsLong();
      if((lv < Integer.MIN_VALUE) || (lv > Integer.MAX_VALUE)) {
        throw new IllegalStateException("Long value '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CSNG = registerFunc(new Func1("CSng") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      Double db = param1.getAsDouble();
      if((db < Float.MIN_VALUE) || (db > Float.MAX_VALUE)) {
        throw new IllegalStateException("Single value '" + db + "' out of range ");
      }
      return BuiltinOperators.toValue(db);
    }
  });

  // FIXME, CSTR, CVAR
  

  // https://www.techonthenet.com/access/functions/
  // https://support.office.com/en-us/article/Access-Functions-by-category-b8b136c3-2716-4d39-94a2-658ce330ed83

  private static Function registerFunc(Function func) {
    return registerFunc(false, func);
  }

  private static Function registerStringFunc(Function func) {
    return registerFunc(true, func);
  }

  private static Function registerFunc(boolean includeNonVar, Function func) {
    String fname = func.getName().toLowerCase();
    if(FUNCS.put(fname, func) != null) {
      throw new IllegalStateException("Duplicate function " + func);
    }
    if(includeNonVar) {
      // for our purposes the non-variant versions are the same function
      fname += NON_VAR_SUFFIX;
      if(FUNCS.put(fname, func) != null) {
        throw new IllegalStateException("Duplicate function " + func);
      }
    }
    return func;
  }
}
