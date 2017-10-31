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

      double result = calculateLoanPayments(rate, pmt, pv, pmtType);

      if(fv != 0d) {
        result += calculateAnnuityPayments(rate, pmt, fv, pmtType);
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
        nper -= calculateLoanPayments(rate, pmt, pv, pmtType);
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
        nper -= calculateAnnuityPayments(rate, pmt, fv, pmtType);
      }

      double result = calculatePresentValue(rate, nper, pmt, pmtType);

      return BuiltinOperators.toValue(result);
    }
  });

  private static double calculateLoanPayments(
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

  private static double calculateAnnuityPayments(
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


}
