/*
Copyright (c) 2018 James Ahlborn

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

import java.text.DateFormat;
import java.util.HashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class FormatUtil
{
  private static final Map<String,Fmt> PREDEF_FMTS = new HashMap<String,Fmt>();

  static {
    PREDEF_FMTS.put("General Date",
                    new PredefDateFmt(TemporalConfig.Type.GENERAL_DATE));
    PREDEF_FMTS.put("Long Date",
                    new PredefDateFmt(TemporalConfig.Type.LONG_DATE));
    PREDEF_FMTS.put("Medium Date",
                    new PredefDateFmt(TemporalConfig.Type.MEDIUM_DATE));
    PREDEF_FMTS.put("Short Date",
                    new PredefDateFmt(TemporalConfig.Type.SHORT_DATE));
    PREDEF_FMTS.put("Long Time",
                    new PredefDateFmt(TemporalConfig.Type.LONG_TIME));
    PREDEF_FMTS.put("Medium Time",
                    new PredefDateFmt(TemporalConfig.Type.MEDIUM_TIME));
    PREDEF_FMTS.put("Short Time",
                    new PredefDateFmt(TemporalConfig.Type.SHORT_TIME));

    PREDEF_FMTS.put("True/False", new PredefBoolFmt("True", "False"));
    PREDEF_FMTS.put("Yes/No", new PredefBoolFmt("Yes", "No"));
    PREDEF_FMTS.put("On/Off", new PredefBoolFmt("On", "Off"));
  }

  private FormatUtil() {}


  public static Value format(EvalContext ctx, Value expr, String fmtStr,
                             int firstDay, int firstWeekType) {


    // FIXME,
    throw new UnsupportedOperationException();
  }

  private static abstract class Fmt
  {
    // FIXME, no null
    public abstract Value format(EvalContext ctx, Value expr, String fmtStr,
                                 int firstDay, int firstWeekType);
  }

  private static class PredefDateFmt extends Fmt
  {
    private final TemporalConfig.Type _type;

    private PredefDateFmt(TemporalConfig.Type type) {
      _type = type;
    }

    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                         int firstDay, int firstWeekType) {
      DateFormat sdf = ctx.createDateFormat(
          ctx.getTemporalConfig().getDateTimeFormat(_type));
      return ValueSupport.toValue(sdf.format(expr.getAsDateTime(ctx)));
    }
  }

  private static class PredefBoolFmt extends Fmt
  {
    private final Value _trueVal;
    private final Value _falseVal;

    private PredefBoolFmt(String trueStr, String falseStr) {
      _trueVal = ValueSupport.toValue(trueStr);
      _falseVal = ValueSupport.toValue(falseStr);
    }

    @Override
    public Value format(EvalContext ctx, Value expr, String fmtStr,
                        int firstDay, int firstWeekType) {
      // FIXME, handle null?
      return(expr.getAsBoolean(ctx) ? _trueVal : _falseVal);
    }
  }


}
