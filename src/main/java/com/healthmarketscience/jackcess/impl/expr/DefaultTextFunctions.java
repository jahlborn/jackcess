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
import com.healthmarketscience.jackcess.expr.LocaleContext;
import com.healthmarketscience.jackcess.expr.Value;
import org.apache.commons.lang.WordUtils;
import static com.healthmarketscience.jackcess.impl.expr.DefaultFunctions.*;
import static com.healthmarketscience.jackcess.impl.expr.FunctionSupport.*;

/**
 *
 * @author James Ahlborn
 */
public class DefaultTextFunctions
{
  // mask to separate the case conversion value (first two bits) from the char
  // conversion value for the StrConv() function
  private static final int STR_CONV_MASK = 0x03;

  private DefaultTextFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }

  public static final Function ASC = registerFunc(new Func1("Asc") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
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
      String str = param1.getAsString(ctx);
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
      int lv = param1.getAsLongInt(ctx);
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
      int lv = param1.getAsLongInt(ctx);
      char[] cs = Character.toChars(lv);
      return ValueSupport.toValue(new String(cs));
    }
  });

  public static final Function STR = registerStringFunc(new Func1NullIsNull("Str") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      BigDecimal bd = param1.getAsBigDecimal(ctx);
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
        start = params[0].getAsLongInt(ctx) - 1;
        ++idx;
      }
      Value param1 = params[idx++];
      if(param1.isNull()) {
        return param1;
      }
      String s1 = param1.getAsString(ctx);
      int s1Len = s1.length();
      if(s1Len == 0) {
        return ValueSupport.ZERO_VAL;
      }
      Value param2 = params[idx++];
      if(param2.isNull()) {
        return param2;
      }
      String s2 = param2.getAsString(ctx);
      int s2Len = s2.length();
      if(s2Len == 0) {
        // 1 based offsets
        return ValueSupport.toValue(start + 1);
      }
      boolean ignoreCase = getIgnoreCase(ctx, params, 3);
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
      String s1 = param1.getAsString(ctx);
      int s1Len = s1.length();
      if(s1Len == 0) {
        return ValueSupport.ZERO_VAL;
      }
      Value param2 = params[1];
      if(param2.isNull()) {
        return param2;
      }
      String s2 = param2.getAsString(ctx);
      int s2Len = s2.length();
      int start = s1Len - 1;
      if(s2Len == 0) {
        // 1 based offsets
        return ValueSupport.toValue(start + 1);
      }
      if(params.length > 2) {
        start = params[2].getAsLongInt(ctx);
        if(start == -1) {
          start = s1Len;
        }
        // 1 based offsets
        --start;
      }
      boolean ignoreCase = getIgnoreCase(ctx, params, 3);
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
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(str.toLowerCase());
    }
  });

  public static final Function UCASE = registerStringFunc(new Func1NullIsNull("UCase") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(str.toUpperCase());
    }
  });

  public static final Function LEFT = registerStringFunc(new Func2("Left") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString(ctx);
      int len = Math.min(str.length(), param2.getAsLongInt(ctx));
      return ValueSupport.toValue(str.substring(0, len));
    }
  });

  public static final Function RIGHT = registerStringFunc(new Func2("Right") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull()) {
        return param1;
      }
      String str = param1.getAsString(ctx);
      int strLen = str.length();
      int len = Math.min(strLen, param2.getAsLongInt(ctx));
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
      String str = param1.getAsString(ctx);
      int strLen = str.length();
      // 1 based offsets
      int start = Math.min(strLen, params[1].getAsLongInt(ctx) - 1);
      int len = Math.min(
          ((params.length > 2) ? params[2].getAsLongInt(ctx) : strLen),
          (strLen - start));
      return ValueSupport.toValue(str.substring(start, start + len));
    }
  });

  public static final Function LEN = registerFunc(new Func1NullIsNull("Len") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(str.length());
    }
  });

  public static final Function LTRIM = registerStringFunc(new Func1NullIsNull("LTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(trim(str, true, false));
    }
  });

  public static final Function RTRIM = registerStringFunc(new Func1NullIsNull("RTrim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(trim(str, false, true));
    }
  });

  public static final Function TRIM = registerStringFunc(new Func1NullIsNull("Trim") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(trim(str, true, true));
    }
  });

  public static final Function REPLACE = registerStringFunc(new FuncVar("Replace", 3, 6) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      String str = params[0].getAsString(ctx);
      String searchStr = params[1].getAsString(ctx);
      String replStr = params[2].getAsString(ctx);

      int strLen = str.length();

      int start = getOptionalIntParam(ctx, params, 3, 1) - 1;
      int count = getOptionalIntParam(ctx, params, 4, -1);
      boolean ignoreCase = getIgnoreCase(ctx, params, 5);

      if(start >= strLen) {
        return ValueSupport.EMPTY_STR_VAL;
      }

      int searchLen = searchStr.length();
      if((searchLen == 0) || (count == 0)) {
        String result = str;
        if(start > 0) {
          result = str.substring(start);
        }
        return ValueSupport.toValue(result);
      }

      if(count < 0) {
        count = strLen;
      }

      StringBuilder result = new StringBuilder(strLen);

      int matchCount = 0;
      for(int i = start; i < strLen; ++i) {
        if((matchCount < count) &&
           str.regionMatches(ignoreCase, i, searchStr, 0, searchLen)) {
          result.append(replStr);
          ++matchCount;
          i += searchLen - 1;
        } else {
          result.append(str.charAt(i));
        }
      }

      return ValueSupport.toValue(result.toString());
    }
  });

  public static final Function SPACE = registerStringFunc(new Func1("Space") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      int lv = param1.getAsLongInt(ctx);
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
      String s1 = param1.getAsString(ctx);
      String s2 = param2.getAsString(ctx);
      boolean ignoreCase = getIgnoreCase(ctx, params, 2);
      int cmp = (ignoreCase ?
                 s1.compareToIgnoreCase(s2) : s1.compareTo(s2));
      // stupid java doesn't return 1, -1, 0...
      return ((cmp < 0) ? ValueSupport.NEG_ONE_VAL :
              ((cmp > 0) ? ValueSupport.ONE_VAL :
               ValueSupport.ZERO_VAL));
    }
  });

  public static final Function STRCONV = registerStringFunc(new FuncVar("StrConv", 2, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      Value param1 = params[0];
      if(param1.isNull()) {
        return ValueSupport.NULL_VAL;
      }

      String str = param1.getAsString(ctx);
      int conversion = params[1].getAsLongInt(ctx);
      // TODO, for now, ignore locale id...?
      // int localeId = params[2];

      int caseConv = STR_CONV_MASK & conversion;
      int charConv = (~STR_CONV_MASK) & conversion;

      switch(caseConv) {
      case 1:
        // vbUpperCase
        str = str.toUpperCase();
        break;
      case 2:
        // vbLowerCase
        str = str.toLowerCase();
        break;
      case 3:
        // vbProperCase
        str = WordUtils.capitalize(str.toLowerCase());
        break;
      default:
        // do nothing
      }

      if(charConv != 0) {
          // 64 = vbUnicode, all java strings are already unicode,so nothing to do
        if(charConv != 64) {
          throw new EvalException("Unsupported character conversion " + charConv);
        }
      }

      return ValueSupport.toValue(str);
    }
  });

  public static final Function STRING = registerStringFunc(new Func2("String") {
    @Override
    protected Value eval2(EvalContext ctx, Value param1, Value param2) {
      if(param1.isNull() || param2.isNull()) {
        return ValueSupport.NULL_VAL;
      }
      int lv = param1.getAsLongInt(ctx);
      char c = (char)(param2.getAsString(ctx).charAt(0) % 256);
      return ValueSupport.toValue(nchars(lv, c));
    }
  });

  public static final Function STRREVERSE = registerFunc(new Func1("StrReverse") {
    @Override
    protected Value eval1(EvalContext ctx, Value param1) {
      String str = param1.getAsString(ctx);
      return ValueSupport.toValue(
          new StringBuilder(str).reverse().toString());
    }
  });

  public static final Function FORMAT = registerFunc(new FuncVar("Format", 1, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {

      Value expr = params[0];
      if(params.length < 2) {
        // no formatting, do simple string conversion
        if(expr.isNull()) {
          return ValueSupport.NULL_VAL;
        }
        return ValueSupport.toValue(expr.getAsString(ctx));
      }

      String fmtStr = params[1].getAsString(ctx);
      int firstDay = DefaultDateFunctions.getFirstDayParam(ctx, params, 2);
      int firstWeekType = DefaultDateFunctions.getFirstWeekTypeParam(ctx, params, 3);
      
      return FormatUtil.format(ctx, expr, fmtStr, firstDay, firstWeekType);
    }
  });
    
  private static String nchars(int num, char c) {
    StringBuilder sb = new StringBuilder(num);
    nchars(sb, num, c);
    return sb.toString();
  }

  static void nchars(StringBuilder sb, int num, char c) {
    for(int i = 0; i < num; ++i) {
      sb.append(c);
    }
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

  private static boolean getIgnoreCase(EvalContext ctx, Value[] params, int idx) {
    boolean ignoreCase = true;
    if(params.length > idx) {
      ignoreCase = doIgnoreCase(ctx, params[idx]);
    }
    return ignoreCase;
  }

  private static boolean doIgnoreCase(LocaleContext ctx, Value paramCmp) {
    int cmpType = paramCmp.getAsLongInt(ctx);
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
