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

import com.healthmarketscience.jackcess.expr.EvalException;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.impl.expr.ExpressionatorTest.eval;
import static com.healthmarketscience.jackcess.impl.expr.ExpressionatorTest.toBD;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFunctionsTest extends TestCase
{

  public DefaultFunctionsTest(String name) {
    super(name);
  }

  // FIXME, test more number/string functions

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
    assertEquals("-42", eval("=Str$(-42)"));
    assertNull(eval("=Str(Null)"));

    try {
      eval("=Str$(Null)");
      fail("EvalException should have been thrown");
    } catch(EvalException expected) {
      // success
    }

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

    assertEquals(-1, eval("=IsNull(Null)"));
    assertEquals(0, eval("=IsNull(13)"));
    assertEquals(-1, eval("=IsDate(#01/02/2003#)"));
    assertEquals(0, eval("=IsDate('foo')"));

    assertEquals(0, eval("=IsNumeric(Null)"));
    assertEquals(0, eval("=IsNumeric('foo')"));
    assertEquals(0, eval("=IsNumeric(#01/02/2003#)"));
    assertEquals(-1, eval("=IsNumeric(37)"));
    assertEquals(-1, eval("=IsNumeric(' 37 ')"));
    assertEquals(-1, eval("=IsNumeric(' -37.5e2 ')"));
    assertEquals(-1, eval("=IsNumeric(' &H37 ')"));
    assertEquals(0, eval("=IsNumeric(' &H37foo ')"));
    assertEquals(0, eval("=IsNumeric(' &o39 ')"));
    assertEquals(-1, eval("=IsNumeric(' &o36 ')"));
    assertEquals(0, eval("=IsNumeric(' &o36.1 ')"));

    assertEquals(1, eval("=VarType(Null)"));
    assertEquals(8, eval("=VarType('blah')"));
    assertEquals(7, eval("=VarType(#01/02/2003#)"));
    assertEquals(3, eval("=VarType(42)"));
    assertEquals(5, eval("=VarType(CDbl(42))"));
    assertEquals(14, eval("=VarType(42.3)"));

    assertEquals("Null", eval("=TypeName(Null)"));
    assertEquals("String", eval("=TypeName('blah')"));
    assertEquals("Date", eval("=TypeName(#01/02/2003#)"));
    assertEquals("Long", eval("=TypeName(42)"));
    assertEquals("Double", eval("=TypeName(CDbl(42))"));
    assertEquals("Decimal", eval("=TypeName(42.3)"));

    assertEquals(2, eval("=InStr('AFOOBAR', 'FOO')"));
    assertEquals(2, eval("=InStr('AFOOBAR', 'foo')"));
    assertEquals(2, eval("=InStr(1, 'AFOOBAR', 'foo')"));
    assertEquals(0, eval("=InStr(1, 'AFOOBAR', 'foo', 0)"));
    assertEquals(2, eval("=InStr(1, 'AFOOBAR', 'foo', 1)"));
    assertEquals(2, eval("=InStr(1, 'AFOOBAR', 'FOO', 0)"));
    assertEquals(2, eval("=InStr(2, 'AFOOBAR', 'FOO')"));
    assertEquals(0, eval("=InStr(3, 'AFOOBAR', 'FOO')"));
    assertEquals(0, eval("=InStr(17, 'AFOOBAR', 'FOO')"));
    assertEquals(2, eval("=InStr(1, 'AFOOBARFOOBAR', 'FOO')"));
    assertEquals(8, eval("=InStr(3, 'AFOOBARFOOBAR', 'FOO')"));
    assertNull(eval("=InStr(3, Null, 'FOO')"));

    assertEquals(2, eval("=InStrRev('AFOOBAR', 'FOO')"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'foo')"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'foo', -1)"));
    assertEquals(0, eval("=InStrRev('AFOOBAR', 'foo', -1, 0)"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'foo', -1, 1)"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'FOO', -1, 0)"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'FOO', 4)"));
    assertEquals(0, eval("=InStrRev('AFOOBAR', 'FOO', 3)"));
    assertEquals(2, eval("=InStrRev('AFOOBAR', 'FOO', 17)"));
    assertEquals(2, eval("=InStrRev('AFOOBARFOOBAR', 'FOO', 9)"));
    assertEquals(8, eval("=InStrRev('AFOOBARFOOBAR', 'FOO', 10)"));
    assertNull(eval("=InStrRev(Null, 'FOO', 3)"));

    assertEquals("FOOO", eval("=UCase(\"fOoO\")"));
    assertEquals("fooo", eval("=LCase(\"fOoO\")"));

    assertEquals("bl", eval("=Left(\"blah\", 2)"));
    assertEquals("", eval("=Left(\"blah\", 0)"));
    assertEquals("blah", eval("=Left(\"blah\", 17)"));
    assertEquals("la", eval("=Mid(\"blah\", 2, 2)"));

    assertEquals("ah", eval("=Right(\"blah\", 2)"));
    assertEquals("", eval("=Right(\"blah\", 0)"));
    assertEquals("blah", eval("=Right(\"blah\", 17)"));

    assertEquals("blah  ", eval("=LTrim(\"  blah  \")"));
    assertEquals("  blah", eval("=RTrim(\"  blah  \")"));
    assertEquals("blah", eval("=Trim(\"  blah  \")"));
    assertEquals("   ", eval("=Space(3)"));
    assertEquals("ddd", eval("=String(3,'d')"));

    assertEquals(1, eval("=StrComp('FOO', 'bar')"));
    assertEquals(-1, eval("=StrComp('bar', 'FOO')"));
    assertEquals(0, eval("=StrComp('FOO', 'foo')"));
    assertEquals(-1, eval("=StrComp('FOO', 'bar', 0)"));
    assertEquals(1, eval("=StrComp('bar', 'FOO', 0)"));
    assertEquals(-1, eval("=StrComp('FOO', 'foo', 0)"));

    assertEquals("FOO", eval("=StrConv('foo', 1)"));
    assertEquals("foo", eval("=StrConv('foo', 2)"));
    assertEquals("foo", eval("=StrConv('FOO', 2)"));
    assertEquals("Foo Bar", eval("=StrConv('FOO bar', 3)"));

    assertEquals("halb", eval("=StrReverse('blah')"));

    assertEquals("foo", eval("=Choose(1,'foo','bar','blah')"));
    assertEquals(null, eval("=Choose(-1,'foo','bar','blah')"));
    assertEquals("blah", eval("=Choose(3,'foo','bar','blah')"));

    assertEquals(null, eval("=Switch(False,'foo', False, 'bar', False, 'blah')"));
    assertEquals("bar", eval("=Switch(False,'foo', True, 'bar', True, 'blah')"));
    assertEquals("blah", eval("=Switch(False,'foo', False, 'bar', True, 'blah')"));

    try {
      eval("=StrReverse('blah', 1)");
      fail("EvalException should have been thrown");
    } catch(EvalException e) {
      assertTrue(e.getMessage().contains("Invalid function call"));
    }

    try {
      eval("=StrReverse()");
      fail("EvalException should have been thrown");
    } catch(EvalException e) {
      assertTrue(e.getMessage().contains("Invalid function call"));
    }
  }

  public void testNumberFuncs() throws Exception
  {
    assertEquals(1, eval("=Abs(1)"));
    assertEquals(1, eval("=Abs(-1)"));
    assertEquals(toBD(1.1), eval("=Abs(-1.1)"));

    assertEquals(Math.atan(0.2), eval("=Atan(0.2)"));
    assertEquals(Math.sin(0.2), eval("=Sin(0.2)"));
    assertEquals(Math.tan(0.2), eval("=Tan(0.2)"));
    assertEquals(Math.cos(0.2), eval("=Cos(0.2)"));

    assertEquals(Math.exp(0.2), eval("=Exp(0.2)"));
    assertEquals(Math.log(0.2), eval("=Log(0.2)"));
    assertEquals(Math.sqrt(4.3), eval("=Sqr(4.3)"));

    assertEquals(3, eval("=Fix(3.5)"));
    assertEquals(4, eval("=Fix(4)"));
    assertEquals(-3, eval("=Fix(-3.5)"));
    assertEquals(-4, eval("=Fix(-4)"));

    assertEquals(1, eval("=Sgn(3.5)"));
    assertEquals(1, eval("=Sgn(4)"));
    assertEquals(-1, eval("=Sgn(-3.5)"));
    assertEquals(-1, eval("=Sgn(-4)"));

    assertEquals(3, eval("=Int(3.5)"));
    assertEquals(4, eval("=Int(4)"));
    assertEquals(-4, eval("=Int(-3.5)"));
    assertEquals(-4, eval("=Int(-4)"));

    assertEquals(toBD(4), eval("=Round(3.7)"));
    assertEquals(4, eval("=Round(4)"));
    assertEquals(toBD(-4), eval("=Round(-3.7)"));
    assertEquals(-4, eval("=Round(-4)"));

    assertEquals(toBD(3.73), eval("=Round(3.7345, 2)"));
    assertEquals(4, eval("=Round(4, 2)"));
    assertEquals(toBD(-3.73), eval("=Round(-3.7345, 2)"));
    assertEquals(-4, eval("=Round(-4, 2)"));
  }

  public void testDateFuncs() throws Exception
  {
    assertEquals("1/2/2003", eval("=CStr(DateValue(#01/02/2003 7:00:00 AM#))"));
    assertEquals("7:00:00 AM", eval("=CStr(TimeValue(#01/02/2003 7:00:00 AM#))"));

    assertEquals("1:10:00 PM", eval("=CStr(#13:10:00#)"));

    assertEquals(2003, eval("=Year(#01/02/2003 7:00:00 AM#)"));
    assertEquals(1, eval("=Month(#01/02/2003 7:00:00 AM#)"));
    assertEquals(2, eval("=Day(#01/02/2003 7:00:00 AM#)"));

    assertEquals(2003, eval("=Year('01/02/2003 7:00:00 AM')"));
    assertEquals(1899, eval("=Year(#7:00:00 AM#)"));

    assertEquals("January", eval("=MonthName(1)"));
    assertEquals("Feb", eval("=MonthName(2,True)"));
    assertEquals("March", eval("=MonthName(3,False)"));

    assertEquals(7, eval("=Hour(#01/02/2003 7:10:27 AM#)"));
    assertEquals(19, eval("=Hour(#01/02/2003 7:10:27 PM#)"));
    assertEquals(10, eval("=Minute(#01/02/2003 7:10:27 AM#)"));
    assertEquals(27, eval("=Second(#01/02/2003 7:10:27 AM#)"));

    assertEquals(7, eval("=Weekday(#11/22/2003#)"));
    assertEquals(3, eval("=Weekday(#11/22/2003#, 5)"));
    assertEquals(1, eval("=Weekday(#11/22/2003#, 7)"));

    assertEquals("Sunday", eval("=WeekdayName(1)"));
    assertEquals("Sun", eval("=WeekdayName(1,True)"));
    assertEquals("Tuesday", eval("=WeekdayName(1,False,3)"));
    assertEquals("Thu", eval("=WeekdayName(3,True,3)"));

    assertTrue(((String)eval("=CStr(Date())"))
                 .matches("\\d{1,2}/\\d{1,2}/\\d{4}"));
    assertTrue(((String)eval("=CStr(Time())"))
               .matches("\\d{1,2}:\\d{2}:\\d{2} (AM|PM)"));

    assertEquals("3:57:34 AM", eval("=CStr(TimeSerial(3,57,34))"));
    assertEquals("3:57:34 PM", eval("=CStr(TimeSerial(15,57,34))"));
    assertEquals("5:45:00 AM", eval("=CStr(TimeSerial(6,-15,0))"));
    assertEquals("12:00:00 AM", eval("=CStr(TimeSerial(0,0,0))"));
    assertEquals("2:00:00 PM", eval("=CStr(TimeSerial(-10,0,0))"));
    assertEquals("6:00:00 AM", eval("=CStr(TimeSerial(30,0,0))"));

    assertEquals("2/12/1969", eval("=CStr(DateSerial(69,2,12))"));
    assertEquals("2/12/2010", eval("=CStr(DateSerial(10,2,12))"));
    assertEquals("7/12/2013", eval("=CStr(DateSerial(2014,-5,12))"));
    assertEquals("8/7/2013", eval("=CStr(DateSerial(2014,-5,38))"));
  }

  public void testFinancialFuncs() throws Exception
  {
    assertEquals("-9.57859403981306",
                 eval("=CStr(NPer(0.12/12,-100,-1000))"));
    assertEquals("-9.48809500550578",
                 eval("=CStr(NPer(0.12/12,-100,-1000,0,1))"));
    assertEquals("60.0821228537616",
                 eval("=CStr(NPer(0.12/12,-100,-1000,10000))"));
    assertEquals("59.6738656742947",
                 eval("=CStr(NPer(0.12/12,-100,-1000,10000,1))"));
    assertEquals("69.6607168935747",
                 eval("=CStr(NPer(0.12/12,-100,0,10000))"));
    assertEquals("69.1619606798005",
                 eval("=CStr(NPer(0.12/12,-100,0,10000,1))"));

    assertEquals("8166.96698564091",
                 eval("=CStr(FV(0.12/12,60,-100))"));
    assertEquals("8248.63665549732",
                 eval("=CStr(FV(0.12/12,60,-100,0,1))"));
    assertEquals("6350.27028707682",
                 eval("=CStr(FV(0.12/12,60,-100,1000))"));
    assertEquals("6431.93995693323",
                 eval("=CStr(FV(0.12/12,60,-100,1000,1))"));

    assertEquals("4495.5038406224",
                 eval("=CStr(PV(0.12/12,60,-100))"));
    assertEquals("4540.45887902863",
                 eval("=CStr(PV(0.12/12,60,-100,0,1))"));
    assertEquals("-1008.99231875519",
                 eval("=CStr(PV(0.12/12,60,-100,10000))"));
    assertEquals("-964.03728034897",
                 eval("=CStr(PV(0.12/12,60,-100,10000,1))"));

    assertEquals("22.2444476849018",
                 eval("=CStr(Pmt(0.12/12,60,-1000))"));
    assertEquals("22.0242056286156",
                 eval("=CStr(Pmt(0.12/12,60,-1000,0,1))"));
    assertEquals("-100.200029164116",
                 eval("=CStr(Pmt(0.12/12,60,-1000,10000))"));
    assertEquals("-99.2079496674414",
                 eval("=CStr(Pmt(0.12/12,60,-1000,10000,1))"));
    assertEquals("-122.444476849018",
                 eval("=CStr(Pmt(0.12/12,60,0,10000))"));
    assertEquals("-121.232155296057",
                 eval("=CStr(Pmt(0.12/12,60,0,10000,1))"));
    assertEquals("22.2444476849018",
                 eval("=CStr(Pmt(0.12/12,60,-1000))"));

    assertEquals("10",
                 eval("=CStr(IPmt(0.12/12,1,60,-1000))"));
    assertEquals("5.90418478297567",
                 eval("=CStr(IPmt(0.12/12,30,60,-1000))"));
    assertEquals("0",
                 eval("=CStr(IPmt(0.12/12,1,60,-1000,0,1))"));
    assertEquals("5.8457275078967",
                 eval("=CStr(IPmt(0.12/12,30,60,-1000,0,1))"));
    assertEquals("0",
                 eval("=CStr(IPmt(0.12/12,1,60,0,10000))"));
    assertEquals("40.9581521702433",
                 eval("=CStr(IPmt(0.12/12,30,60,0,10000))"));
    assertEquals("0",
                 eval("=CStr(IPmt(0.12/12,1,60,0,10000,1))"));
    assertEquals("40.552625911132",
                 eval("=CStr(IPmt(0.12/12,30,60,0,10000,1))"));
    assertEquals("10",
                 eval("=CStr(IPmt(0.12/12,1,60,-1000,10000))"));
    assertEquals("46.862336953219",
                 eval("=CStr(IPmt(0.12/12,30,60,-1000,10000))"));
    assertEquals("0",
                 eval("=CStr(IPmt(0.12/12,1,60,-1000,10000,1))"));
    assertEquals("46.3983534190287",
                 eval("=CStr(IPmt(0.12/12,30,60,-1000,10000,1))"));

    assertEquals("12.2444476849018",
                 eval("=CStr(PPmt(0.12/12,1,60,-1000))"));
    assertEquals("16.3402629019261",
                 eval("=CStr(PPmt(0.12/12,30,60,-1000))"));
    assertEquals("22.0242056286156",
                 eval("=CStr(PPmt(0.12/12,1,60,-1000,0,1))"));
    assertEquals("16.1784781207189",
                 eval("=CStr(PPmt(0.12/12,30,60,-1000,0,1))"));
    assertEquals("-122.444476849018",
                 eval("=CStr(PPmt(0.12/12,1,60,0,10000))"));
    assertEquals("-163.402629019261",
                 eval("=CStr(PPmt(0.12/12,30,60,0,10000))"));
    assertEquals("-121.232155296057",
                 eval("=CStr(PPmt(0.12/12,1,60,0,10000,1))"));
    assertEquals("-161.784781207189",
                 eval("=CStr(PPmt(0.12/12,30,60,0,10000,1))"));
    assertEquals("-110.200029164116",
                 eval("=CStr(PPmt(0.12/12,1,60,-1000,10000))"));
    assertEquals("-147.062366117335",
                 eval("=CStr(PPmt(0.12/12,30,60,-1000,10000))"));
    assertEquals("-99.2079496674414",
                 eval("=CStr(PPmt(0.12/12,1,60,-1000,10000,1))"));
    assertEquals("-145.60630308647",
                 eval("=CStr(PPmt(0.12/12,30,60,-1000,10000,1))"));

    assertEquals("1.31506849315068",
                 eval("=CStr(DDB(2400,300,10*365,1))"));
    assertEquals("40",
                 eval("=CStr(DDB(2400,300,10*12,1))"));
    assertEquals("480",
                 eval("=CStr(DDB(2400,300,10,1))"));
    assertEquals("22.1225472000002",
                 eval("=CStr(DDB(2400,300,10,10))"));
    assertEquals("245.76",
                 eval("=CStr(DDB(2400,300,10,4))"));
    assertEquals("307.2",
                 eval("=CStr(DDB(2400,300,10,3))"));
    assertEquals("480",
                 eval("=CStr(DDB(2400,300,10,0.1))"));
    assertEquals("274.768033075174",
                 eval("=CStr(DDB(2400,300,10,3.5))"));

    assertEquals("2250",
                 eval("=CStr(SLN(30000,7500,10))"));
    assertEquals("1000",
                 eval("=CStr(SLN(10000,5000,5))"));
    assertEquals("1142.85714285714",
                 eval("=CStr(SLN(8000,0,7))"));

    assertEquals("4090.90909090909",
                 eval("=CStr(SYD(30000,7500,10,1))"));
    assertEquals("409.090909090909",
                 eval("=CStr(SYD(30000,7500,10,10))"));

    assertEquals("-1.63048347266756E-02",
                 eval("=CStr(Rate(3,200,-610,0,-20,0.1))"));
    assertEquals("7.70147248820175E-03",
                 eval("=CStr(Rate(4*12,-200,8000))"));
    assertEquals("-1.09802980531205",
                 eval("=CStr(Rate(60,93.22,5000,0.1))"));
  }

}
