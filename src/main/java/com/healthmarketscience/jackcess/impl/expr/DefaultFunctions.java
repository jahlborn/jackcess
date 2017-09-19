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
      super(name, 0, 0);
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

  public static abstract class Func1NullIsNull extends BaseFunction
  {
    protected Func1NullIsNull(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      Value param1 = params[0];
      if(param1.isNull()) {
        return param1;
      }
      return eval1(ctx, param1);
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

  public static abstract class FuncVar extends BaseFunction
  {
    protected FuncVar(String name, int minParams, int maxParams) {
      super(name, minParams, maxParams);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return evalVar(ctx, params);
    }

    protected abstract Value evalVar(EvalContext ctx, Value[] params);
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

  public static final Function HEX = registerStringFunc(new Func1NullIsNull("Hex") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      if((param1.getType() == Value.Type.STRING) &&
         (param1.getAsString().length() == 0)) {
        return BuiltinOperators.ZERO_VAL;
      }
      long lv = param1.getAsLong();
      return BuiltinOperators.toValue(Long.toHexString(lv).toUpperCase());
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

  public static final Function OCT = registerStringFunc(new Func1NullIsNull("Oct") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
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
      long lv = roundToLong(param1);
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
      bd = bd.setScale(4, RoundingMode.HALF_EVEN);
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
      long lv = roundToLong(param1);
      if((lv < Short.MIN_VALUE) || (lv > Short.MAX_VALUE)) {
        throw new IllegalStateException("Int value '" + lv + "' out of range ");
      }
      return BuiltinOperators.toValue(lv);
    }
  });

  public static final Function CLNG = registerFunc(new Func1("CLng") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      long lv = roundToLong(param1);
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
      return BuiltinOperators.toValue((double)db.floatValue());
    }
  });

  public static final Function CSTR = registerFunc(new Func1("CStr") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      return BuiltinOperators.toValue(param1.getAsString());
    }
  });

  // FIXME, CVAR

  public static final Function INSTR = registerFunc(new FuncVar("InStr", 2, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      int idx = 0;
      int start = 0;
      if(params.length > 2) {
        // 1 based offsets
        start = params[0].getAsLong().intValue() - 1;
        ++idx;
      }
      Value param1 = params[idx++];
      if(param1.isNull()) {
        return param1;
      }
      String s1 = param1.getAsString();
      int s1Len = s1.length();
      if(s1Len == 0) {
        return BuiltinOperators.ZERO_VAL;
      }
      Value param2 = params[idx++];
      if(param2.isNull()) {
        return param2;
      }
      String s2 = param2.getAsString();
      int s2Len = s2.length();
      if(s2Len == 0) {
        // 1 based offsets
        return BuiltinOperators.toValue(start + 1);
      }
      boolean ignoreCase = true;
      if(params.length > 3) {
        ignoreCase = doIgnoreCase(params[3]);
      }
      int end = s1Len - s2Len;
      while(start < end) {
        if(s1.regionMatches(ignoreCase, start, s2, 0, s2Len)) {
          // 1 based offsets
          return BuiltinOperators.toValue(start + 1);
        } 
        ++start;
      }
      return BuiltinOperators.ZERO_VAL;
    }
  });

  public static final Function INSTRREV = registerFunc(new FuncVar("InStrRev", 2, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return param1;
      }
      String s1 = param1.getAsString();
      int s1Len = s1.length();
      if(s1Len == 0) {
        return BuiltinOperators.ZERO_VAL;
      }
      Value param2 = params[1];
      if(param2.isNull()) {
        return param2;
      }
      String s2 = param2.getAsString();
      int s2Len = s2.length();
      int start = s1Len - 1;
      if(s2Len == 0) {
        // 1 based offsets
        return BuiltinOperators.toValue(start + 1);
      }
      if(params.length > 2) {
        start = params[2].getAsLong().intValue();
        if(start == -1) {
          start = s1Len;
        } 
        // 1 based offsets
        --start;
      }
      boolean ignoreCase = true;
      if(params.length > 3) {
        ignoreCase = doIgnoreCase(params[3]);
      }
      start = Math.min(s1Len - s2Len, start);
      while(start >= 0) {
        if(s1.regionMatches(ignoreCase, start, s2, 0, s2Len)) {
          // 1 based offsets
          return BuiltinOperators.toValue(start + 1);
        } 
        --start;
      }
      return BuiltinOperators.ZERO_VAL;
    }
  });

  public static final Function LCASE = registerStringFunc(new Func1NullIsNull("LCase") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(str.toLowerCase());
    }
  });

  public static final Function UCASE = registerStringFunc(new Func1NullIsNull("UCase") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(str.toUpperCase());
    }
  });

  public static final Function LEFT = registerStringFunc(new Func2("Left") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString();
      int len = (int)Math.min(str.length(), param2.getAsLong());
      return BuiltinOperators.toValue(str.substring(0, len));
    }
  });

  public static final Function RIGHT = registerStringFunc(new Func2("Right") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString();
      int strLen = str.length();
      int len = (int)Math.min(strLen, param2.getAsLong());
      return BuiltinOperators.toValue(str.substring(strLen - len, strLen));
    }
  });

  public static final Function MID = registerStringFunc(new FuncVar("Mid", 2, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString();
      int strLen = str.length();
      // 1 based offsets
      int start = (int)Math.max(strLen, params[1].getAsLong() - 1);
      int len = Math.max(
          ((params.length > 2) ? params[2].getAsLong().intValue() : strLen),
          (strLen - start));
      return BuiltinOperators.toValue(str.substring(start, start + len));
    }
  });

  public static final Function LEN = registerFunc(new Func1NullIsNull("Len") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(str.length());
    }
  });

  public static final Function LTRIM = registerStringFunc(new Func1NullIsNull("LTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(trim(str, true, false));
    }
  });

  public static final Function RTRIM = registerStringFunc(new Func1NullIsNull("RTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(trim(str, false, true));
    }
  });

  public static final Function TRIM = registerStringFunc(new Func1NullIsNull("Trim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(trim(str, true, true));
    }
  });

  public static final Function SPACE = registerStringFunc(new Func1("Space") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLong().intValue();
      return BuiltinOperators.toValue(nchars(lv, ' '));
    }
  });

  public static final Function STRCOMP = registerFunc(new FuncVar("StrComp", 2, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      Value param2 = params[1];
      if(param1.isNull() || param2.isNull()) {
        return BuiltinOperators.NULL_VAL;
      }
      String s1 = param1.getAsString();
      String s2 = param2.getAsString();
      boolean ignoreCase = true;
      if(params.length > 2) {
        ignoreCase = doIgnoreCase(params[2]);
      }
      int cmp = (ignoreCase ? 
                 s1.compareToIgnoreCase(s2) : s1.compareTo(s2));
      return BuiltinOperators.toValue(cmp);
    }
  });

  public static final Function STRING = registerStringFunc(new Func2("String") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull() || param2.isNull()) {
        return BuiltinOperators.NULL_VAL;
      }
      int lv = param1.getAsLong().intValue();
      char c = (char)(param2.getAsString().charAt(0) % 256);
      return BuiltinOperators.toValue(nchars(lv, c));
    }
  });

  public static final Function STRREVERSE = registerFunc(new Func1("StrReverse") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return BuiltinOperators.toValue(
          new StringBuilder(str).reverse().toString());
    }
  });


  private static String nchars(int num, char c) {
    StringBuilder sb = new StringBuilder(num);
    for(int i = 0; i < num; ++i) {
      sb.append(c);
    }
    return sb.toString();
  }

  private static String trim(String str, boolean doLeft, boolean doRight) {
    int start = 0;
    int end = str.length();

    if(doLeft) {
      while((start < end) && (str.charAt(start) == ' ')) {
        ++start;
      }
    } 
    if(doRight) {
      while((start < end) && (str.charAt(end - 1) == ' ')) {
        --end;
      }
    }
    return str.substring(start, end);
  }

  private static boolean doIgnoreCase(Value paramCmp) {
    int cmpType = paramCmp.getAsLong().intValue();
    switch(cmpType) {
    case -1:
      // vbUseCompareOption -> default is binary
    case 0:
      // vbBinaryCompare
      return false;
    case 1:
      // vbTextCompare
      return true;
    default:
      // vbDatabaseCompare -> unsupported
      throw new IllegalStateException("Unsupported compare type " + cmpType);
    } 
  }

  private static long roundToLong(Value param) {
    if(param.getType().isIntegral()) {
      return param.getAsLong();
    }
    return param.getAsBigDecimal().setScale(0, RoundingMode.HALF_EVEN)
      .longValue();
  }

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
