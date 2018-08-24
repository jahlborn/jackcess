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
  /** 0 - payment end of month (default) */
  private static final int PMT_END_MNTH = 0;
  /** 1 - payment start of month */
  private static final int PMT_BEG_MNTH = 1;


  private DefaultFinancialFunctions() {}

  static void init() {
    // dummy method to ensure this class is loaded
  }


  public static final Function NPER = registerFunc(new FuncVar("NPer", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble();
      double pmt = params[1].getAsDouble();
      double pv = params[2].getAsDouble();

      double fv = 0d;
      if(params.length > 3) {
        fv = params[3].getAsDouble();
      }

      int pmtType = PMT_END_MNTH;
      if(params.length > 4) {
        pmtType = params[4].getAsLongInt();
      }

      double result = calculateLoanPaymentPeriods(rate, pmt, pv, pmtType);

      if(fv != 0d) {
        result += calculateAnnuityPaymentPeriods(rate, pmt, fv, pmtType);
      }

      return BuiltinOperators.toValue(result);
    }
  });

  public static final Function FV = registerFunc(new FuncVar("FV", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble();
      double nper = params[1].getAsDouble();
      double pmt = params[2].getAsDouble();

      double pv = 0d;
      if(params.length > 3) {
        pv = params[3].getAsDouble();
      }

      int pmtType = PMT_END_MNTH;
      if(params.length > 4) {
        pmtType = params[4].getAsLongInt();
      }

      if(pv != 0d) {
        nper -= calculateLoanPaymentPeriods(rate, pmt, pv, pmtType);
      }

      double result = calculateFutureValue(rate, nper, pmt, pmtType);

      return BuiltinOperators.toValue(result);
    }
  });

  public static final Function PV = registerFunc(new FuncVar("PV", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble();
      double nper = params[1].getAsDouble();
      double pmt = params[2].getAsDouble();

      double fv = 0d;
      if(params.length > 3) {
        fv = params[3].getAsDouble();
      }

      int pmtType = PMT_END_MNTH;
      if(params.length > 4) {
        pmtType = params[4].getAsLongInt();
      }

      if(fv != 0d) {
        nper -= calculateAnnuityPaymentPeriods(rate, pmt, fv, pmtType);
      }

      double result = calculatePresentValue(rate, nper, pmt, pmtType);

      return BuiltinOperators.toValue(result);
    }
  });

  public static final Function PMT = registerFunc(new FuncVar("Pmt", 3, 5) {
    @Override
    protected Value evalVar(EvalContext ctx, Value[] params) {
      double rate = params[0].getAsDouble();
      double nper = params[1].getAsDouble();
      double pv = params[2].getAsDouble();

      double fv = 0d;
      if(params.length > 3) {
        fv = params[3].getAsDouble();
      }

      int pmtType = PMT_END_MNTH;
      if(params.length > 4) {
        pmtType = params[4].getAsLongInt();
      }

      double result = calculateLoanPayment(rate, nper, pv, pmtType);

      if(fv != 0d) {
        result += calculateAnnuityPayment(rate, nper, fv, pmtType);
      }

      return BuiltinOperators.toValue(result);
    }
  });

  // FIXME not working for all param combos
  // public static final Function IPMT = registerFunc(new FuncVar("IPmt", 4, 6) {
  //   @Override
  //   protected Value evalVar(EvalContext ctx, Value[] params) {
  //     double rate = params[0].getAsDouble();
  //     double per = params[1].getAsDouble();
  //     double nper = params[2].getAsDouble();
  //     double pv = params[3].getAsDouble();

  //     double fv = 0d;
  //     if(params.length > 4) {
  //       fv = params[4].getAsDouble();
  //     }

  //     int pmtType = PMT_END_MNTH;
  //     if(params.length > 5) {
  //       pmtType = params[5].getAsLongInt();
  //     }

  //     double pmt = calculateLoanPayment(rate, nper, pv, pmtType);

  //     if(fv != 0d) {
  //       pmt += calculateAnnuityPayment(rate, nper, fv, pmtType);
  //     }

  //     double result = calculateInterestPayment(pmt, rate, per, pv, pmtType);

  //     return BuiltinOperators.toValue(result);
  //   }
  // });

  // FIXME untested
  // public static final Function PPMT = registerFunc(new FuncVar("PPmt", 4, 6) {
  //   @Override
  //   protected Value evalVar(EvalContext ctx, Value[] params) {
  //     double rate = params[0].getAsDouble();
  //     double per = params[1].getAsDouble();
  //     double nper = params[2].getAsDouble();
  //     double pv = params[3].getAsDouble();

  //     double fv = 0d;
  //     if(params.length > 4) {
  //       fv = params[4].getAsDouble();
  //     }

  //     int pmtType = PMT_END_MNTH;
  //     if(params.length > 5) {
  //       pmtType = params[5].getAsLongInt();
  //     }

  //     double pmt = calculateLoanPayment(rate, nper, pv, pmtType);

  //     if(fv != 0d) {
  //       pmt += calculateAnnuityPayment(rate, nper, fv, pmtType);
  //     }

  //     double result = pmt - calculateInterestPayment(pmt, rate, per, pv,
  //                                                    pmtType);

  //     return BuiltinOperators.toValue(result);
  //   }
  // });

  // FIXME, doesn't work for partial days
  // public static final Function DDB = registerFunc(new FuncVar("DDB", 4, 5) {
  //   @Override
  //   protected Value evalVar(EvalContext ctx, Value[] params) {
  //     double cost = params[0].getAsDouble();
  //     double salvage = params[1].getAsDouble();
  //     double life = params[2].getAsDouble();
  //     double period = params[3].getAsDouble();

  //     double factor = 2d;
  //     if(params.length > 4) {
  //       factor = params[4].getAsDouble();
  //     }

  //     double result = 0d;

  //     // fractional value always rounds up to one year
  //     if(period < 1d) {
  //       period = 1d;
  //     }

  //     // FIXME? apply partial period _first_
  //     // double partPeriod = period % 1d;
  //     // if(partPeriod != 0d) {
  //     //   result = calculateDoubleDecliningBalance(
  //     //       cost, salvage, life, factor) * partPeriod;
  //     //   period -= partPeriod;
  //     //   cost -= result;
  //     // }
  //     double prevResult = 0d;
  //     while(period > 0d) {
  //       prevResult = result;
  //       double remPeriod = Math.min(period, 1d);
  //       result = calculateDoubleDecliningBalance(
  //           cost, salvage, life, factor);
  //       if(remPeriod < 1d) {
  //         result = (prevResult + result) / 2d;
  //       }
  //       period -= 1d;
  //       cost -= result;
  //     }

  //     return BuiltinOperators.toValue(result);
  //   }
  // });

  // FIXME, untested
  // public static final Function SLN = registerFunc(new FuncVar("SLN", 3, 3) {
  //   @Override
  //   protected Value evalVar(EvalContext ctx, Value[] params) {
  //     double cost = params[0].getAsDouble();
  //     double salvage = params[1].getAsDouble();
  //     double life = params[2].getAsDouble();

  //     double result = calculateStraightLineDepreciation(cost, salvage, life);

  //     return BuiltinOperators.toValue(result);
  //   }
  // });

  // FIXME, untested
  // public static final Function SYD = registerFunc(new FuncVar("SYD", 4, 4) {
  //   @Override
  //   protected Value evalVar(EvalContext ctx, Value[] params) {
  //     double cost = params[0].getAsDouble();
  //     double salvage = params[1].getAsDouble();
  //     double life = params[2].getAsDouble();
  //     double period = params[3].getAsDouble();

  //     double result = calculateSumOfYearsDepreciation(
  //         cost, salvage, life, period);

  //     return BuiltinOperators.toValue(result);
  //   }
  // });


  private static double calculateLoanPaymentPeriods(
      double rate, double pmt, double pv, int pmtType) {

    // https://brownmath.com/bsci/loan.htm
    // http://financeformulas.net/Number-of-Periods-of-Annuity-from-Present-Value.html

    if(pmtType == PMT_BEG_MNTH) {
      pv += pmt;
    }

    double v1 = Math.log(1d + (rate * pv / pmt));

    double v2 = Math.log(1d + rate);

    double result = -v1 / v2;

    if(pmtType == PMT_BEG_MNTH) {
      result += 1d;
    }

    return result;
  }

  private static double calculateAnnuityPaymentPeriods(
      double rate, double pmt, double fv, int pmtType) {

    // https://brownmath.com/bsci/loan.htm
    // http://financeformulas.net/Number-of-Periods-of-Annuity-from-Future-Value.html
    // https://accountingexplained.com/capital/tvm/fv-annuity

    if(pmtType == PMT_BEG_MNTH) {
      fv *= (1d + rate);
    }

    double v1 = Math.log(1d - (rate * fv / pmt));

    double v2 = Math.log(1d + rate);

    double result = v1 / v2;

    if(pmtType == PMT_BEG_MNTH) {
      result -= 1d;
    }

    return result;
  }

  private static double calculateFutureValue(
      double rate, double nper, double pmt, int pmtType) {

    double result = -pmt * ((Math.pow((1d + rate), nper) - 1d) / rate);

    if(pmtType == PMT_BEG_MNTH) {
      result *= (1d + rate);
    }

    return result;
  }

  private static double calculatePresentValue(
      double rate, double nper, double pmt, int pmtType) {

    if(pmtType == PMT_BEG_MNTH) {
      nper -= 1d;
    }

    double result = -pmt * ((1d - Math.pow((1d + rate), -nper)) / rate);

    if(pmtType == PMT_BEG_MNTH) {
      result -= pmt;
    }

    return result;
  }

  private static double calculateLoanPayment(
      double rate, double nper, double pv, int pmtType) {

    double result = -(rate * pv) / (1d - Math.pow((1d + rate), -nper));

    if(pmtType == PMT_BEG_MNTH) {
      result /= (1d + rate);
    }

    return result;
  }

  private static double calculateAnnuityPayment(
      double rate, double nper, double fv, int pmtType) {

    double result = -(fv * rate) / (Math.pow((1d + rate), nper) - 1d);

    if(pmtType == PMT_BEG_MNTH) {
      result /= (1d + rate);
    }

    return result;
  }

  private static double calculateInterestPayment(
      double pmt, double rate, double per, double pv, int pmtType) {

    // http://www.tvmcalcs.com/index.php/calculators/apps/excel_loan_amortization
    // http://financeformulas.net/Remaining_Balance_Formula.html

    double pvPer = per;
    double fvPer = per;
    if(pmtType == PMT_END_MNTH) {
      pvPer -= 1d;
      fvPer -= 1d;
    } else {
      pvPer -= 2d;
      fvPer -= 1d;
    }

    double remBalance = (pv * Math.pow((1d + rate), pvPer)) -
      // FIXME, always use pmtType of 0?
      calculateFutureValue(rate, fvPer, pmt, PMT_END_MNTH);

    double result = -(remBalance * rate);

    return result;
  }

  private static double calculateDoubleDecliningBalance(
      double cost, double salvage, double life, double factor) {

    double result1 = cost * (factor/life);
    double result2 = cost - salvage;

    return Math.min(result1, result2);
  }

  private static double calculateStraightLineDepreciation(
      double cost, double salvage, double life) {
    return ((cost - salvage) / life);
  }

  private static double calculateSumOfYearsDepreciation(
      double cost, double salvage, double life, double period) {

    double sumOfYears = (period * (period + 1)) / 2d;
    double result = ((cost - salvage) * ((life + 1 - period) / sumOfYears));

    return result;
  }

}
