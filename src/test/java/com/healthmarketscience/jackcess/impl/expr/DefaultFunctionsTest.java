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

import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.TestUtil;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.impl.expr.ExpressionatorTest.eval;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFunctionsTest extends TestCase 
{

  public DefaultFunctionsTest(String name) {
    super(name);
  }

  public void testFuncs() throws Exception
  {
    assertEquals("foo", eval("=IIf(10 > 1, \"foo\", \"bar\")"));
    assertEquals("bar", eval("=IIf(10 < 1, \"foo\", \"bar\")"));
    assertEquals(102, eval("=Asc(\"foo\")"));
    assertEquals(9786, eval("=AscW(\"\u263A\")"));
    assertEquals("f", eval("=Chr(102)"));
    assertEquals("\u263A", eval("=ChrW(9786)"));
    assertEquals("263A", eval("=Hex(9786)"));

    assertEquals("blah", eval("=Nz(\"blah\")"));
    assertEquals("", eval("=Nz(Null)"));
    assertEquals("blah", eval("=Nz(\"blah\",\"FOO\")"));
    assertEquals("FOO", eval("=Nz(Null,\"FOO\")"));

    assertEquals("23072", eval("=Oct(9786)"));
    assertEquals(" 9786", eval("=Str(9786)"));
    assertEquals("-42", eval("=Str(-42)"));

    assertEquals(-1, eval("=CBool(\"1\")"));
    assertEquals(13, eval("=CByte(\"13\")"));
    assertEquals(14, eval("=CByte(\"13.7\")"));
    assertEquals(new BigDecimal("57.1235"), eval("=CCur(\"57.12346\")"));
    assertEquals(new Double("57.12345"), eval("=CDbl(\"57.12345\")"));
    assertEquals(new BigDecimal("57.123456789"), eval("=CDec(\"57.123456789\")"));
    assertEquals(513, eval("=CInt(\"513\")"));
    assertEquals(514, eval("=CInt(\"513.7\")"));
    assertEquals(345513, eval("=CLng(\"345513\")"));
    assertEquals(345514, eval("=CLng(\"345513.7\")"));
    assertEquals(new Float("57.12345").doubleValue(),
                 eval("=CSng(\"57.12345\")"));
    assertEquals("9786", eval("=CStr(9786)"));
    assertEquals("-42", eval("=CStr(-42)"));
    
    // FIXME, instr, instrrev

    assertEquals("FOOO", eval("=UCase(\"fOoO\")"));
    assertEquals("fooo", eval("=LCase(\"fOoO\")"));
    
    assertEquals("bl", eval("=Left(\"blah\", 2)"));
    assertEquals("", eval("=Left(\"blah\", 0)"));
    assertEquals("blah", eval("=Left(\"blah\", 17)"));

    assertEquals("ah", eval("=Right(\"blah\", 2)"));
    assertEquals("", eval("=Right(\"blah\", 0)"));
    assertEquals("blah", eval("=Right(\"blah\", 17)"));

  }


  public void testFinancialFuncs() throws Exception
  {
    assertEquals("-9.578594039813165", 
                 eval("=CStr(NPer(0.12/12,-100,-1000))"));
    assertEquals("-9.488095005505832", 
                 eval("=CStr(NPer(0.12/12,-100,-1000,0,1))"));
    assertEquals("60.08212285376166", 
                 eval("=CStr(NPer(0.12/12,-100,-1000,10000))"));
    assertEquals("59.673865674294554", 
                 eval("=CStr(NPer(0.12/12,-100,-1000,10000,1))"));
    assertEquals("69.66071689357483", 
                 eval("=CStr(NPer(0.12/12,-100,0,10000))"));
    assertEquals("69.16196067980039", 
                 eval("=CStr(NPer(0.12/12,-100,0,10000,1))"));

    assertEquals("8166.966985640913", 
                 eval("=CStr(FV(0.12/12,60,-100))"));
    assertEquals("8248.636655497321", 
                 eval("=CStr(FV(0.12/12,60,-100,0,1))"));
    assertEquals("6350.270287076823", 
                 eval("=CStr(FV(0.12/12,60,-100,1000))"));
    assertEquals("6431.939956933231", 
                 eval("=CStr(FV(0.12/12,60,-100,1000,1))"));

    assertEquals("4495.503840622403", 
                 eval("=CStr(PV(0.12/12,60,-100))"));
    assertEquals("4540.458879028627", 
                 eval("=CStr(PV(0.12/12,60,-100,0,1))"));
    assertEquals("-1008.992318755193", 
                 eval("=CStr(PV(0.12/12,60,-100,10000))"));
    assertEquals("-964.0372803489684", 
                 eval("=CStr(PV(0.12/12,60,-100,10000,1))"));

    assertEquals("22.24444768490176", 
                 eval("=CStr(Pmt(0.12/12,60,-1000))"));
    assertEquals("22.024205628615604", 
                 eval("=CStr(Pmt(0.12/12,60,-1000,0,1))"));
    assertEquals("-100.20002916411586", 
                 eval("=CStr(Pmt(0.12/12,60,-1000,10000))"));
    assertEquals("-99.20794966744144", 
                 eval("=CStr(Pmt(0.12/12,60,-1000,10000,1))"));
    assertEquals("-122.44447684901762", 
                 eval("=CStr(Pmt(0.12/12,60,0,10000))"));
    assertEquals("-121.23215529605704", 
                 eval("=CStr(Pmt(0.12/12,60,0,10000,1))"));

    // FIXME not working for all param combos
    // assertEquals("10.0", 
    //              eval("=CStr(IPmt(0.12/12,1,60,-1000))"));
    // assertEquals("5.904184782975672", 
    //              eval("=CStr(IPmt(0.12/12,30,60,-1000))"));
    // 0
    // assertEquals("", 
    //              eval("=CStr(IPmt(0.12/12,1,60,-1000,0,1))"));
    // 5.84572750...
    // assertEquals("5.845727507896704", 
    //              eval("=CStr(IPmt(0.12/12,30,60,-1000,0,1))"));
    // 0
    // assertEquals("", 
    //              eval("=CStr(IPmt(0.12/12,1,60,0,10000))"));
    // 40.9581521702433
    // assertEquals("40.95815217024329", 
    //              eval("=CStr(IPmt(0.12/12,30,60,0,10000))"));
    // 0
    // assertEquals("", 
    //              eval("=CStr(IPmt(0.12/12,1,60,0,10000,1))"));
    // 40.552625911132
    // assertEquals("40.55262591113197", 
    //              eval("=CStr(IPmt(0.12/12,30,60,0,10000,1))"));
    // assertEquals("10.0", 
    //              eval("=CStr(IPmt(0.12/12,1,60,-1000,10000))"));
    // assertEquals("46.862336953218964", 
    //              eval("=CStr(IPmt(0.12/12,30,60,-1000,10000))"));
    // 0
    // assertEquals("", 
    //              eval("=CStr(IPmt(0.12/12,1,60,-1000,10000,1))"));
    // 46.3983534190287
    // assertEquals("46.39835341902867", 
    //              eval("=CStr(IPmt(0.12/12,30,60,-1000,10000,1))"));

    // FIXME, doesn't work for partial days
    // assertEquals("1.3150684931506849", 
    //              eval("=CStr(DDB(2400,300,10*365,1))"));
    // assertEquals("40.0", 
    //              eval("=CStr(DDB(2400,300,10*12,1))"));
    // assertEquals("480.0", 
    //              eval("=CStr(DDB(2400,300,10,1))"));
    // assertEquals("22.122547200000042", 
    //              eval("=CStr(DDB(2400,300,10,10))"));
    // assertEquals("245.76", 
    //              eval("=CStr(DDB(2400,300,10,4))"));
    // assertEquals("307.20000000000005", 
    //              eval("=CStr(DDB(2400,300,10,3))"));
    // assertEquals("480.0", 
    //              eval("=CStr(DDB(2400,300,10,0.1))"));
    // 274.768033075174
    // assertEquals("", 
    //              eval("=CStr(DDB(2400,300,10,3.5))"));

    
  }

}
