/*
Copyright (c) 2017 James Ahlborn

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

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.Value;
import static com.healthmarketscience.jackcess.impl.expr.DefaultFunctions.*;
import static com.healthmarketscience.jackcess.impl.expr.FunctionSupport.*;

/**
 *
 * @author James Ahlborn
 */
public class DefaultTextFunctions
{

  private DefaultTextFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function ASC = registerFunc(new Func1("Asc") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      int len = str.length();
      if(len == 0) {
        throw new EvalException("No characters in string");
      }
      int lv = str.charAt(0);
      if((lv < 0) || (lv > 255)) {
        throw new EvalException("Character code '" + lv +
                                        "' out of range ");
      }
      return ValueSupport.toValue(lv);
    }
  });

  public static final Function ASCW = registerFunc(new Func1("AscW") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      int len = str.length();
      if(len == 0) {
        throw new EvalException("No characters in string");
      }
      int lv = str.charAt(0);
      return ValueSupport.toValue(lv);
    }
  });

  public static final Function CHR = registerStringFunc(new Func1NullIsNull("Chr") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      if((lv < 0) || (lv > 255)) {
        throw new EvalException("Character code '" + lv +
                                        "' out of range ");
      }
      char[] cs = Character.toChars(lv);
      return ValueSupport.toValue(new String(cs));
    }
  });

  public static final Function CHRW = registerStringFunc(new Func1NullIsNull("ChrW") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      char[] cs = Character.toChars(lv);
      return ValueSupport.toValue(new String(cs));
    }
  });

  public static final Function STR = registerStringFunc(new Func1NullIsNull("Str") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal();
      String str = bd.toPlainString();
      if(bd.compareTo(BigDecimal.ZERO) >= 0) {
        str = " " + str;
      }
      return ValueSupport.toValue(str);
    }
  });

  public static final Function INSTR = registerFunc(new FuncVar("InStr", 2, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      int idx = 0;
      int start = 0;
      if(params.length > 2) {
        // 1 based offsets
        start = params[0].getAsLongInt() - 1;
        ++idx;
      }
      Value param1 = params[idx++];
      if(param1.isNull()) {
        return param1;
      }
      String s1 = param1.getAsString();
      int s1Len = s1.length();
      if(s1Len == 0) {
        return ValueSupport.ZERO_VAL;
      }
      Value param2 = params[idx++];
      if(param2.isNull()) {
        return param2;
      }
      String s2 = param2.getAsString();
      int s2Len = s2.length();
      if(s2Len == 0) {
        // 1 based offsets
        return ValueSupport.toValue(start + 1);
      }
      boolean ignoreCase = true;
      if(params.length > 3) {
        ignoreCase = doIgnoreCase(params[3]);
      }
      int end = s1Len - s2Len;
      while(start < end) {
        if(s1.regionMatches(ignoreCase, start, s2, 0, s2Len)) {
          // 1 based offsets
          return ValueSupport.toValue(start + 1);
        }
        ++start;
      }
      return ValueSupport.ZERO_VAL;
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
        return ValueSupport.ZERO_VAL;
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
        return ValueSupport.toValue(start + 1);
      }
      if(params.length > 2) {
        start = params[2].getAsLongInt();
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
      start = Math.min(s1Len - s2Len, start - s2Len + 1);
      while(start >= 0) {
        if(s1.regionMatches(ignoreCase, start, s2, 0, s2Len)) {
          // 1 based offsets
          return ValueSupport.toValue(start + 1);
        }
        --start;
      }
      return ValueSupport.ZERO_VAL;
    }
  });

  public static final Function LCASE = registerStringFunc(new Func1NullIsNull("LCase") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(str.toLowerCase());
    }
  });

  public static final Function UCASE = registerStringFunc(new Func1NullIsNull("UCase") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(str.toUpperCase());
    }
  });

  public static final Function LEFT = registerStringFunc(new Func2("Left") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString();
      int len = Math.min(str.length(), param2.getAsLongInt());
      return ValueSupport.toValue(str.substring(0, len));
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
      int len = Math.min(strLen, param2.getAsLongInt());
      return ValueSupport.toValue(str.substring(strLen - len, strLen));
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
      int start = Math.min(strLen, params[1].getAsLongInt() - 1);
      int len = Math.min(
          ((params.length > 2) ? params[2].getAsLongInt() : strLen),
          (strLen - start));
      return ValueSupport.toValue(str.substring(start, start + len));
    }
  });

  public static final Function LEN = registerFunc(new Func1NullIsNull("Len") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(str.length());
    }
  });

  public static final Function LTRIM = registerStringFunc(new Func1NullIsNull("LTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(trim(str, true, false));
    }
  });

  public static final Function RTRIM = registerStringFunc(new Func1NullIsNull("RTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(trim(str, false, true));
    }
  });

  public static final Function TRIM = registerStringFunc(new Func1NullIsNull("Trim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(trim(str, true, true));
    }
  });

  public static final Function SPACE = registerStringFunc(new Func1("Space") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt();
      return ValueSupport.toValue(nchars(lv, ' '));
    }
  });

  public static final Function STRCOMP = registerFunc(new FuncVar("StrComp", 2, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      Value param2 = params[1];
      if(param1.isNull() || param2.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      String s1 = param1.getAsString();
      String s2 = param2.getAsString();
      boolean ignoreCase = true;
      if(params.length > 2) {
        ignoreCase = doIgnoreCase(params[2]);
      }
      int cmp = (ignoreCase ?
                 s1.compareToIgnoreCase(s2) : s1.compareTo(s2));
      // stupid java doesn't return 1, -1, 0...
      return ((cmp < 0) ? ValueSupport.NEG_ONE_VAL :
              ((cmp > 0) ? ValueSupport.ONE_VAL :
               ValueSupport.ZERO_VAL));
    }
  });

  public static final Function STRING = registerStringFunc(new Func2("String") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull() || param2.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      int lv = param1.getAsLongInt();
      char c = (char)(param2.getAsString().charAt(0) % 256);
      return ValueSupport.toValue(nchars(lv, c));
    }
  });

  public static final Function STRREVERSE = registerFunc(new Func1("StrReverse") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString();
      return ValueSupport.toValue(
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
    int cmpType = paramCmp.getAsLongInt();
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
      throw new EvalException("Unsupported compare type " + cmpType);
    }
  }


}
