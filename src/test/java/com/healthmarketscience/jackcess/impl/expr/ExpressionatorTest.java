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
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.script.Bindings;
import javax.script.SimpleBindings;

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.TestUtil;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.FunctionLookup;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.NumberFormatter;
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

    validateExpr("-42", "<EUnaryOp>{- <ELiteralValue>{42}}");

    validateExpr("(+37)", "<EParen>{(<EUnaryOp>{+ <ELiteralValue>{37}})}");

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

    validateExpr("' \"A\" '", "<ELiteralValue>{\" \"\"A\"\" \"}",
                 "\" \"\"A\"\" \"");
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
    for(int i = -10; i <= 10; ++i) {
      assertEquals(-i, eval("=-(" + i + ")"));
    }

    for(int i = -10; i <= 10; ++i) {
      assertEquals(i, eval("=+(" + i + ")"));
    }

    for(double i : DBLS) {
      assertEquals(toBD(-i), eval("=-(" + i + ")"));
    }

    for(double i : DBLS) {
      assertEquals(toBD(i), eval("=+(" + i + ")"));
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        assertEquals((i + j), eval("=" + i + " + " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals(toBD(toBD(i).add(toBD(j))), eval("=" + i + " + " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        assertEquals((i - j), eval("=" + i + " - " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals(toBD(toBD(i).subtract(toBD(j))), eval("=" + i + " - " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        assertEquals((i * j), eval("=" + i + " * " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        assertEquals(toBD(toBD(i).multiply(toBD(j))), eval("=" + i + " * " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0L) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          assertEquals((i / j), eval("=" + i + " \\ " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(roundToLongInt(j) == 0) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          assertEquals((roundToLongInt(i) / roundToLongInt(j)),
                       eval("=" + i + " \\ " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          assertEquals((i % j), eval("=" + i + " Mod " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(roundToLongInt(j) == 0) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          assertEquals((roundToLongInt(i) % roundToLongInt(j)),
                       eval("=" + i + " Mod " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0) {
          evalFail("=" + i + " / " + j, ArithmeticException.class);
        } else {
          double result = (double)i / (double)j;
          if((int)result == result) {
            assertEquals((int)result, eval("=" + i + " / " + j));
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
          assertEquals(toBD(BuiltinOperators.divide(toBD(i), toBD(j))),
                       eval("=" + i + " / " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        double result = Math.pow(i, j);
        if((int)result == result) {
          assertEquals((int)result, eval("=" + i + " ^ " + j));
        } else {
          assertEquals(result, eval("=" + i + " ^ " + j));
        }
      }
    }
  }

  public void testTrickyMathExpressions() throws Exception
  {
    assertEquals(37, eval("=30+7"));
    assertEquals(23, eval("=30+-7"));
    assertEquals(23, eval("=30-+7"));
    assertEquals(37, eval("=30--7"));
    assertEquals(23, eval("=30-7"));

    assertEquals(100, eval("=-10^2"));
    assertEquals(-100, eval("=-(10)^2"));
    assertEquals(-100d, eval("=-\"10\"^2"));
    assertEquals(toBD(-98.9d), eval("=1.1+(-\"10\"^2)"));

    assertEquals(toBD(99d), eval("=-10E-1+10e+1"));
    assertEquals(toBD(-101d), eval("=-10E-1-10e+1"));
  }

  public void testTypeCoercion() throws Exception
  {
    assertEquals("foobar", eval("=\"foo\" + \"bar\""));

    assertEquals("12foo", eval("=12 + \"foo\""));
    assertEquals("foo12", eval("=\"foo\" + 12"));

    assertEquals(37d, eval("=\"25\" + 12"));
    assertEquals(37d, eval("=12 + \"25\""));

    evalFail(("=12 - \"foo\""), RuntimeException.class);
    evalFail(("=\"foo\" - 12"), RuntimeException.class);

    assertEquals("foo1225", eval("=\"foo\" + 12 + 25"));
    assertEquals("37foo", eval("=12 + 25 + \"foo\""));
    assertEquals("foo37", eval("=\"foo\" + (12 + 25)"));
    assertEquals("25foo12", eval("=\"25foo\" + 12"));

    assertEquals(new Date(1485579600000L), eval("=#1/1/2017# + 27"));
    assertEquals(128208, eval("=#1/1/2017# * 3"));
  }

  public void testLikeExpression() throws Exception
  {
    validateExpr("Like \"[abc]*\"", "<ELikeOp>{<EThisValue>{<THIS_COL>} Like \"[abc]*\"([abc].*)}",
                 "<THIS_COL> Like \"[abc]*\"");
    assertTrue(evalCondition("Like \"[abc]*\"", "afcd"));
    assertFalse(evalCondition("Like \"[abc]*\"", "fcd"));

    validateExpr("Like \"[abc*\"", "<ELikeOp>{<EThisValue>{<THIS_COL>} Like \"[abc*\"((?!))}",
                 "<THIS_COL> Like \"[abc*\"");
    assertFalse(evalCondition("Like \"[abc*\"", "afcd"));
    assertFalse(evalCondition("Like \"[abc*\"", "fcd"));
    assertFalse(evalCondition("Like \"[abc*\"", ""));
  }

  public void testLiteralDefaultValue() throws Exception
  {
    assertEquals("-28 blah ", eval("=CDbl(9)-37 & \" blah \"",
                                     Value.Type.STRING));
    assertEquals("CDbl(9)-37 & \" blah \"",
                 eval("CDbl(9)-37 & \" blah \"", Value.Type.STRING));

    assertEquals(-28d, eval("=CDbl(9)-37", Value.Type.DOUBLE));
    assertEquals(-28d, eval("CDbl(9)-37", Value.Type.DOUBLE));
  }

  private static void validateExpr(String exprStr, String debugStr) {
    validateExpr(exprStr, debugStr, exprStr);
  }

  private static void validateExpr(String exprStr, String debugStr,
                                   String cleanStr) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.FIELD_VALIDATOR, exprStr, null,
        new TestParseContext());
    String foundDebugStr = expr.toDebugString();
    if(foundDebugStr.startsWith("<EImplicitCompOp>")) {
      assertEquals("<EImplicitCompOp>{<EThisValue>{<THIS_COL>} = " +
                   debugStr + "}", foundDebugStr);
    } else {
      assertEquals(debugStr, foundDebugStr);
    }
    assertEquals(cleanStr, expr.toString());
  }

  static Object eval(String exprStr) {
    return eval(exprStr, null);
  }

  static Object eval(String exprStr, Value.Type resultType) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, resultType,
        new TestParseContext());
    return expr.eval(new TestEvalContext(null));
  }

  private static void evalFail(
      String exprStr, Class<? extends Exception> failure)
  {
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, null,
        new TestParseContext());
    try {
      expr.eval(new TestEvalContext(null));
      fail(failure + " should have been thrown");
    } catch(Exception e) {
      assertTrue(failure.isInstance(e));
    }
  }

  private static Boolean evalCondition(String exprStr, String thisVal) {
    Expression expr = Expressionator.parse(
        Expressionator.Type.FIELD_VALIDATOR, exprStr, null,
        new TestParseContext());
    return (Boolean)expr.eval(new TestEvalContext(BuiltinOperators.toValue(thisVal)));
  }

  static int roundToLongInt(double d) {
    return new BigDecimal(d).setScale(0, NumberFormatter.ROUND_MODE)
      .intValueExact();
  }

  static BigDecimal toBD(double d) {
    return toBD(BigDecimal.valueOf(d));
  }

  static BigDecimal toBD(BigDecimal bd) {
    return BuiltinOperators.normalize(bd);
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
    public FunctionLookup getFunctionLookup() {
      return DefaultFunctions.LOOKUP;
    }
  }

  private static final class TestEvalContext implements EvalContext
  {
    private final Value _thisVal;
    private final RandomContext _rndCtx = new RandomContext();
    private final Bindings _bindings = new SimpleBindings();

    private TestEvalContext(Value thisVal) {
      _thisVal = thisVal;
    }

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
      if(_thisVal == null) {
        throw new UnsupportedOperationException();
      }
      return _thisVal;
    }

    public Value getIdentifierValue(Identifier identifier) {
      throw new UnsupportedOperationException();
    }

    public float getRandom(Integer seed) {
      return _rndCtx.getRandom(seed);
    }

    public Bindings getBindings() {
      return _bindings;
    }

    public Object get(String key) {
      return _bindings.get(key);
    }

    public void put(String key, Object value) {
      _bindings.put(key, value);
    }
  }
}
