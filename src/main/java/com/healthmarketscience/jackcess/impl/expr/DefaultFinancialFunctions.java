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

NOTICE:
Many of the financial functions have been originally copied from the Apache
POI project (Apache Software Foundation) and the UCanAccess Project.  They
have been then modified and adapted so that they are integrated with Jackcess,
in a consistent manner.  The Apache POI and UCanAccess projects are licensed
under Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.

*/

package com.healthmarketscience.jackcess.impl.expr;


import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.Value;
import static com.healthmarketscience.jackcess.impl.expr.DefaultFunctions.*;
import static com.healthmarketscience.jackcess.impl.expr.FunctionSupport.*;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFinancialFunctions
{
  // Useful Sources:
  // https://brownmath.com/bsci/loan.htm
  // http://financeformulas.net/Number-of-Periods-of-Annuity-from-Future-Value.html
  // https://accountingexplained.com/capital/tvm/fv-annuity
  // http://www.tvmcalcs.com/index.php/calculators/apps/excel_loan_amortization


  /** 0 - payment end of month (default) */
  private static final int PMT_END_MNTH = 0;
  /** 1 - payment start of month */
  private static final int PMT_BEG_MNTH = 1;

  private static final int MAX_RATE_ITERATIONS = 20;
  private static final double RATE_PRECISION = 0.0000001;// 1.0e-8


  private DefaultFinancialFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }


  public static final Function NPER = registerFunc(new FuncVar("NPer", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double pmt = params[1].getAsDouble(ctx);
      double pv = params[2].getAsDouble(ctx);
      double fv = getFV(ctx, params, 3);
      int pmtType = getPaymentType(ctx, params, 4);

      double result = calculateLoanPaymentPeriods(rate, pmt, pv, fv, pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function FV = registerFunc(new FuncVar("FV", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double nper = params[1].getAsDouble(ctx);
      double pmt = params[2].getAsDouble(ctx);
      double pv = getOptionalDoubleParam(ctx, params, 3, 0d);
      int pmtType = getPaymentType(ctx, params, 4);

      double result = calculateFutureValue(rate, nper, pmt, pv, pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function PV = registerFunc(new FuncVar("PV", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double nper = params[1].getAsDouble(ctx);
      double pmt = params[2].getAsDouble(ctx);
      double fv = getFV(ctx, params, 3);
      int pmtType = getPaymentType(ctx, params, 4);

      double result = calculatePresentValue(rate, nper, pmt, fv, pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function PMT = registerFunc(new FuncVar("Pmt", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double nper = params[1].getAsDouble(ctx);
      double pv = params[2].getAsDouble(ctx);
      double fv = getFV(ctx, params, 3);
      int pmtType = getPaymentType(ctx, params, 4);

      double result = calculateLoanPayment(rate, nper, pv, fv, pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function IPMT = registerFunc(new FuncVar("IPmt", 4, 6) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double per = params[1].getAsDouble(ctx);
      double nper = params[2].getAsDouble(ctx);
      double pv = params[3].getAsDouble(ctx);
      double fv = getFV(ctx, params, 4);
      int pmtType = getPaymentType(ctx, params, 5);

      double result = calculateInterestPayment(rate, per, nper, pv, fv,
                                                  pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function PPMT = registerFunc(new FuncVar("PPmt", 4, 6) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble(ctx);
      double per = params[1].getAsDouble(ctx);
      double nper = params[2].getAsDouble(ctx);
      double pv = params[3].getAsDouble(ctx);
      double fv = getFV(ctx, params, 4);
      int pmtType = getPaymentType(ctx, params, 5);

      double result = calculatePrincipalPayment(rate, per, nper, pv, fv,
                                                   pmtType);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function DDB = registerFunc(new FuncVar("DDB", 4, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double cost = params[0].getAsDouble(ctx);
      double salvage = params[1].getAsDouble(ctx);
      double life = params[2].getAsDouble(ctx);
      double period = params[3].getAsDouble(ctx);
      double factor = getOptionalDoubleParam(ctx, params, 4, 2d);

      double result = calculateDoubleDecliningBalance(
          cost, salvage, life, period, factor);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function SLN = registerFunc(new FuncVar("SLN", 3, 3) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double cost = params[0].getAsDouble(ctx);
      double salvage = params[1].getAsDouble(ctx);
      double life = params[2].getAsDouble(ctx);

      double result = calculateStraightLineDepreciation(cost, salvage, life);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function SYD = registerFunc(new FuncVar("SYD", 4, 4) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double cost = params[0].getAsDouble(ctx);
      double salvage = params[1].getAsDouble(ctx);
      double life = params[2].getAsDouble(ctx);
      double period = params[3].getAsDouble(ctx);

      double result = calculateSumOfYearsDepreciation(
          cost, salvage, life, period);

      return ValueSupport.toValue(result);
    }
  });

  public static final Function Rate = registerFunc(new FuncVar("Rate", 3, 6) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double nper = params[0].getAsDouble(ctx);
      double pmt = params[1].getAsDouble(ctx);
      double pv = params[2].getAsDouble(ctx);
      double fv = getFV(ctx, params, 3);
      int pmtType = getPaymentType(ctx, params, 4);
      double guess = getOptionalDoubleParam(ctx, params, 5, 0.1);

      double result = calculateRate(nper, pmt, pv, fv, pmtType, guess);

      return ValueSupport.toValue(result);
    }
  });

  private static double calculateFutureValue(
      double rate, double nper, double pmt, double pv, int pmtType) {
    if (rate == 0d) {
      return -1 * (pv + (nper * pmt));
    }
    double r1 = (pmtType == PMT_BEG_MNTH ? (rate + 1) : 1);
    double p1 = Math.pow((rate + 1), nper);
    return ((((1 - p1) * r1 * pmt) / rate) - (pv * p1));

  }

  private static double calculatePresentValue(
      double rate, double nper, double pmt, double fv, int pmtType) {
    if (rate == 0d) {
      return -1 * ((nper * pmt) + fv);
    }
    double r1 = (pmtType == PMT_BEG_MNTH ? (rate + 1) : 1);
    double p1 = Math.pow((rate + 1), nper);
    return ((((1 - p1) / rate) * r1 * pmt) - fv) / p1;
  }

  private static double calculateLoanPayment(
      double rate, double nper, double pv, double fv, int pmtType) {

    if (rate == 0d) {
      return -1*(fv + pv) / nper;
    }
    double r1 = (pmtType == PMT_BEG_MNTH ? (rate + 1) : 1);
    double p1 = Math.pow((rate + 1), nper);
    return (fv + (pv * p1)) * rate / (r1 * (1 - p1));
  }


  private static double calculateInterestPayment(
      double rate, double per, double nper, double pv, double fv, int pmtType) {

    if((per == 1d) && (pmtType == PMT_BEG_MNTH)) {
      // no inerest for pmt at beginning of month of 1st period
      return 0d;
    }

    double pmt = calculateLoanPayment(rate, nper, pv, fv, pmtType);
    double result = calculateFutureValue(
        rate, per - 1, pmt, pv, pmtType) * rate;

    if (pmtType == PMT_BEG_MNTH) {
      result /= (1 + rate);
    }
    return result;
  }

  private static double calculatePrincipalPayment(
      double rate, double per, double nper, double pv, double fv, int pmtType) {
    double pmt = calculateLoanPayment(rate, nper, pv, fv, pmtType);
    double ipmt = calculateInterestPayment(rate, per, nper, pv, fv, pmtType);
    return (pmt - ipmt);
  }

  public static double calculateDoubleDecliningBalance(
      double cost, double salvage, double life, double period, double factor) {
    if (cost < 0 || ((life == 2d) && (period > 1d))) {
      return 0;
    }
    if (life < 2d || ((life == 2d) && (period <= 1d))) {
      return (cost - salvage);
    }

    double v1 = ((factor * cost) / life);
    if (period <= 1d) {
      return Math.min(v1, cost - salvage);
    }

    double v2 = (life - factor) / life;
    double v3 = Math.max(salvage - (cost * Math.pow(v2, period)), 0);
    double result = (v1 * Math.pow(v2, period - 1d)) - v3;

    return Math.max(result, 0);
  }

  private static double calculateStraightLineDepreciation(
      double cost, double salvage, double life) {
    return ((cost - salvage) / life);
  }

  private static double calculateSumOfYearsDepreciation(
      double cost, double salvage, double life, double period) {
    return ((cost - salvage) * (life - period + 1) * 2d) /
      (life * (life + 1));
  }

  private static double calculateLoanPaymentPeriods(
      double rate, double pmt, double pv, double fv, int pmtType) {

    if (rate == 0d) {
      return -1 * (fv + pv) / pmt;
    }

    double cr = ((pmtType == PMT_BEG_MNTH) ? (1 + rate) : 1) * pmt / rate;
    double v1;
    double v2;
    if((cr - fv) < 0d) {
      v1 = Math.log(fv - cr);
      v2 = Math.log(-pv - cr);
    } else {
      v1 = Math.log(cr - fv);
      v2 = Math.log(pv + cr);
    }

    return (v1 - v2) / Math.log(1 + rate);
  }

  public static double calculateRate(double nper, double pmt, double pv,
                                     double fv, int pmtType, double guess) {
    double y, f = 0;
    double rate = guess;
    if (Math.abs(rate) < RATE_PRECISION) {
      y = pv * (1 + nper * rate) + pmt * (1 + rate * pmtType) * nper + fv;
    } else {
      f = Math.exp(nper * Math.log(1 + rate));
      y = pv * f + pmt * (1 / rate + pmtType) * (f - 1) + fv;
    }
    double y0 = pv + pmt * nper + fv;
    double y1 = pv * f + pmt * (1 / rate + pmtType) * (f - 1) + fv;

    // find root by Newton secant method
    int i = 0;
    double x0 = 0.0;
    double x1 = rate;
    while ((Math.abs(y0 - y1) > RATE_PRECISION) && (i < MAX_RATE_ITERATIONS)) {
      rate = (y1 * x0 - y0 * x1) / (y1 - y0);
      x0 = x1;
      x1 = rate;

      if (Math.abs(rate) < RATE_PRECISION) {
        y = pv * (1 + nper * rate) + pmt * (1 + rate * pmtType) * nper + fv;
      } else {
        f = Math.exp(nper * Math.log(1 + rate));
        y = pv * f + pmt * (1 / rate + pmtType) * (f - 1) + fv;
      }

      y0 = y1;
      y1 = y;
      ++i;
    }

    return rate;
  }

  private static double getFV(EvalContext ctx, Value[] params, int idx) {
    return getOptionalDoubleParam(ctx, params, idx, 0d);
  }

  private static int getPaymentType(EvalContext ctx, Value[] params, int idx) {
    int pmtType = PMT_END_MNTH;
    if(params.length > idx) {
      pmtType = (params[idx].getAsLongInt(ctx) != PMT_END_MNTH) ?
        PMT_BEG_MNTH : PMT_END_MNTH;
    }
    return pmtType;
  }
}
