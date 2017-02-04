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

import java.text.SimpleDateFormat;

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.TestUtil;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class ExpressionatorTest extends TestCase 
{
  private static final double[] DBLS = {
    -10.3d,-9.0d,-8.234d,-7.11111d,-6.99999d,-5.5d,-4.0d,-3.4159265d,-2.84d,
    -1.0000002d,-1.0d,-0.0002013d,0.0d, 0.9234d,1.0d,1.954d,2.200032d,3.001d,
    4.9321d,5.0d,6.66666d,7.396d,8.1d,9.20456200d,10.325d};

  public ExpressionatorTest(String name) {
    super(name);
  }


  public void testParseSimpleExprs() throws Exception
  {
    validateExpr("\"A\"", "<ELiteralValue>{\"A\"}");
    
    validateExpr("13", "<ELiteralValue>{13}");

    validateExpr("-42", "<ELiteralValue>{-42}");

    doTestSimpleBinOp("EBinaryOp", "+", "-", "*", "/", "\\", "^", "&", "Mod");
    doTestSimpleBinOp("ECompOp", "<", "<=", ">", ">=", "=", "<>");
    doTestSimpleBinOp("ELogicalOp", "And", "Or", "Eqv", "Xor", "Imp");

    for(String constStr : new String[]{"True", "False", "Null"}) {
      validateExpr(constStr, "<EConstValue>{" + constStr + "}");
    }

    validateExpr("[Field1]", "<EObjValue>{[Field1]}");

    validateExpr("[Table2].[Field3]", "<EObjValue>{[Table2].[Field3]}");

    validateExpr("Not \"A\"", "<EUnaryOp>{Not <ELiteralValue>{\"A\"}}");

    validateExpr("-[Field1]", "<EUnaryOp>{- <EObjValue>{[Field1]}}");

    validateExpr("\"A\" Is Null", "<ENullOp>{<ELiteralValue>{\"A\"} Is Null}");

    validateExpr("\"A\" In (1,2,3)", "<EInOp>{<ELiteralValue>{\"A\"} In (<ELiteralValue>{1},<ELiteralValue>{2},<ELiteralValue>{3})}");

    validateExpr("\"A\" Not Between 3 And 7", "<EBetweenOp>{<ELiteralValue>{\"A\"} Not Between <ELiteralValue>{3} And <ELiteralValue>{7}}");

    validateExpr("(\"A\" Or \"B\")", "<EParen>{(<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ELiteralValue>{\"B\"}})}");

    validateExpr("IIf(\"A\",42,False)", "<EFunc>{IIf(<ELiteralValue>{\"A\"},<ELiteralValue>{42},<EConstValue>{False})}");

    validateExpr("\"A\" Like \"a*b\"", "<ELikeOp>{<ELiteralValue>{\"A\"} Like \"a*b\"(a.*b)}");
  }

  private static void doTestSimpleBinOp(String opName, String... ops) throws Exception
  {
    for(String op : ops) {
      validateExpr("\"A\" " + op + " \"B\"", 
                   "<" + opName + ">{<ELiteralValue>{\"A\"} " + op +
                   " <ELiteralValue>{\"B\"}}");
    }
  }

  public void testOrderOfOperations() throws Exception
  {
    validateExpr("\"A\" Eqv \"B\"", 
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELiteralValue>{\"B\"}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\"", 
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELiteralValue>{\"C\"}}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\" Or \"D\"", 
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELogicalOp>{<ELiteralValue>{\"C\"} Or <ELiteralValue>{\"D\"}}}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\" Or \"D\" And \"E\"", 
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELogicalOp>{<ELiteralValue>{\"C\"} Or <ELogicalOp>{<ELiteralValue>{\"D\"} And <ELiteralValue>{\"E\"}}}}}");

    validateExpr("\"A\" Or \"B\" Or \"C\"", 
                 "<ELogicalOp>{<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ELiteralValue>{\"B\"}} Or <ELiteralValue>{\"C\"}}");

    validateExpr("\"A\" & \"B\" Is Null", 
                 "<ENullOp>{<EBinaryOp>{<ELiteralValue>{\"A\"} & <ELiteralValue>{\"B\"}} Is Null}");

    validateExpr("\"A\" Or \"B\" Is Null", 
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ENullOp>{<ELiteralValue>{\"B\"} Is Null}}");

    validateExpr("Not \"A\" & \"B\"", 
                 "<EUnaryOp>{Not <EBinaryOp>{<ELiteralValue>{\"A\"} & <ELiteralValue>{\"B\"}}}");

    validateExpr("Not \"A\" Or \"B\"", 
                 "<ELogicalOp>{<EUnaryOp>{Not <ELiteralValue>{\"A\"}} Or <ELiteralValue>{\"B\"}}");

    validateExpr("\"A\" + \"B\" Not Between 37 - 15 And 52 / 4", 
                 "<EBetweenOp>{<EBinaryOp>{<ELiteralValue>{\"A\"} + <ELiteralValue>{\"B\"}} Not Between <EBinaryOp>{<ELiteralValue>{37} - <ELiteralValue>{15}} And <EBinaryOp>{<ELiteralValue>{52} / <ELiteralValue>{4}}}");

    validateExpr("\"A\" + (\"B\" Not Between 37 - 15 And 52) / 4", 
                 "<EBinaryOp>{<ELiteralValue>{\"A\"} + <EBinaryOp>{<EParen>{(<EBetweenOp>{<ELiteralValue>{\"B\"} Not Between <EBinaryOp>{<ELiteralValue>{37} - <ELiteralValue>{15}} And <ELiteralValue>{52}})} / <ELiteralValue>{4}}}");


  }

  public void testSimpleMathExpressions() throws Exception
  {
    for(long i = -10L; i <= 10L; ++i) {
      assertEquals(-i, eval("=-(" + i + ")"));
    }

    for(double i : DBLS) {
      assertEquals(-i, eval("=-(" + i + ")"));
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        assertEquals((i + j), eval("=" + i + " + " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals((i + j), eval("=" + i + " + " + j));
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        assertEquals((i - j), eval("=" + i + " - " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals((i - j), eval("=" + i + " - " + j));
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        assertEquals((i * j), eval("=" + i + " * " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals((i * j), eval("=" + i + " * " + j));
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        if(j == 0L) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          assertEquals((i / j), eval("=" + i + " \\ " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if((long)j == 0L) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          assertEquals(((long)i / (long)j), eval("=" + i + " \\ " + j));
        }
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        if(j == 0L) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          assertEquals((i % j), eval("=" + i + " Mod " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if((long)j == 0L) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          assertEquals(((long)i % (long)j), eval("=" + i + " Mod " + j));
        }
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        if(j == 0L) {
          evalFail("=" + i + " / " + j, ArithmeticException.class);
        } else {
          double result = (double)i / (double)j;
          if((long)result == result) {
            assertEquals((long)result, eval("=" + i + " / " + j));
          } else {
            assertEquals(result, eval("=" + i + " / " + j));
          }
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(j == 0.0d) {
          evalFail("=" + i + " / " + j, ArithmeticException.class);
        } else {
          assertEquals((i / j), eval("=" + i + " / " + j));
        }
      }
    }

    for(long i = -10L; i <= 10L; ++i) {
      for(long j = -10L; j <= 10L; ++j) {
        double result = Math.pow(i, j);
        if((long)result == result) {
          assertEquals((long)result, eval("=" + i + " ^ " + j));
        } else {
          assertEquals(result, eval("=" + i + " ^ " + j));
        }
      }
    }


  }

  private static void validateExpr(String exprStr, String debugStr) {
    validateExpr(exprStr, debugStr, exprStr);
  }

  private static void validateExpr(String exprStr, String debugStr, 
                                   String cleanStr) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.FIELD_VALIDATOR, exprStr, null);
    assertEquals(debugStr, expr.toDebugString());
    assertEquals(cleanStr, expr.toString());
  }

  private static Object eval(String exprStr) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, new TestParseContext());
    return expr.eval(new TestEvalContext());
  }

  private static void evalFail(String exprStr, Class<? extends Exception> failure) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, new TestParseContext());
    try {
      expr.eval(new TestEvalContext());
      fail(failure + " should have been thrown");
    } catch(Exception e) {
      assertTrue(failure.isInstance(e));
    }
  }

  private static final class TestParseContext implements Expressionator.ParseContext
  {
    public TemporalConfig getTemporalConfig() {
      return TemporalConfig.US_TEMPORAL_CONFIG;
    }
    public SimpleDateFormat createDateFormat(String formatStr) {
      SimpleDateFormat sdf = DatabaseBuilder.createDateFormat(formatStr);
      sdf.setTimeZone(TestUtil.TEST_TZ);
      return sdf;
    }

    public Function getExpressionFunction(String name) {
      return null;
    }
  }

  private static final class TestEvalContext implements EvalContext
  {
    public Value.Type getResultType() {
      return null;
    }

    public TemporalConfig getTemporalConfig() {
      return TemporalConfig.US_TEMPORAL_CONFIG;
    }

    public SimpleDateFormat createDateFormat(String formatStr) {
      SimpleDateFormat sdf = DatabaseBuilder.createDateFormat(formatStr);
      sdf.setTimeZone(TestUtil.TEST_TZ);
      return sdf;
    }

    public Value getThisColumnValue() {
      throw new UnsupportedOperationException();
    }

    public Value getRowValue(String collectionName, String objName,
                             String colName) {
      throw new UnsupportedOperationException();
    }
  }
}
