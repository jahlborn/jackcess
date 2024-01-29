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
import java.time.LocalDateTime;
import java.util.Calendar;

import com.healthmarketscience.jackcess.expr.EvalException;
import junit.framework.AssertionFailedError;
import org.junit.Test;

import static com.healthmarketscience.jackcess.impl.expr.ExpressionatorTest.eval;
import static com.healthmarketscience.jackcess.impl.expr.ExpressionatorTest.toBD;
import static junit.framework.TestCase.*;

/**
 *
 * @author James Ahlborn / Markus Spann
 */
public class DefaultFunctionsTest
{

  @Test
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
    assertEquals(LocalDateTime.of(2003,1,2,0,0), eval("=CDate('01/02/2003')"));
    assertEquals(LocalDateTime.of(2003,1,2,7,0), eval("=CDate('01/02/2003 7:00:00 AM')"));
    assertEquals(LocalDateTime.of(1908,3,31,10,48), eval("=CDate(3013.45)"));


    assertEquals(-1, eval("=IsNull(Null)"));
    assertEquals(0, eval("=IsNull(13)"));
    assertEquals(-1, eval("=IsDate(#01/02/2003#)"));
    assertEquals(0, eval("=IsDate('foo')"));
    assertEquals(0, eval("=IsDate('200')"));

    assertEquals(0, eval("=IsNumeric(Null)"));
    assertEquals(0, eval("=IsNumeric('foo')"));
    assertEquals(0, eval("=IsNumeric(#01/02/2003#)"));
    assertEquals(0, eval("=IsNumeric('01/02/2003')"));
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
    assertEquals(" FOO \" BAR ", eval("=UCase(\" foo \"\" bar \")"));

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

    assertEquals(1615198d, eval("=Val('    1615 198th Street N.E.')"));
    assertEquals(-1d, eval("=Val('  &HFFFFwhatever')"));
    assertEquals(131071d, eval("=Val('  &H1FFFFwhatever')"));
    assertEquals(-1d, eval("=Val('  &HFFFFFFFFwhatever')"));
    assertEquals(291d, eval("=Val('  &H123whatever')"));
    assertEquals(83d, eval("=Val('  &O123whatever')"));
    assertEquals(1.23d, eval("=Val('  1 2 3 e -2 whatever')"));
    assertEquals(0d, eval("=Val('  whatever123 ')"));
    assertEquals(0d, eval("=Val('')"));

    assertEquals("faa", eval("=Replace('foo','o','a')"));
    assertEquals("faa", eval("=Replace('fOo','o','a')"));
    assertEquals("aa", eval("=Replace('foo','o','a',2)"));
    assertEquals("oo", eval("=Replace('foo','o','a',2,0)"));
    assertEquals("", eval("=Replace('foo','o','a',4)"));
    assertEquals("foo", eval("=Replace('foo','','a')"));
    assertEquals("o", eval("=Replace('foo','','a',3)"));
    assertEquals("fahhabahhaahha", eval("=Replace('fooboooo','OO','ahha')"));
    assertEquals("fahhaboooo", eval("=Replace('fooboooo','OO','ahha',1,1)"));
    assertEquals("fooboooo", eval("=Replace('fooboooo','OO','ahha',1,1,0)"));
    assertEquals("ahhabahhaahha", eval("=Replace('fooboooo','OO','ahha',2)"));
    assertEquals("obahhaahha", eval("=Replace('fooboooo','OO','ahha',3)"));
    assertEquals("fb", eval("=Replace('fooboooo','OO','')"));
    assertEquals("", eval("=Replace('','o','a')"));
    assertEquals("foo", eval("=Replace('foo','foobar','a')"));

    assertEquals("12,345.00", eval("=FormatNumber(12345)"));
    assertEquals("0.12", eval("=FormatNumber(0.12345)"));
    assertEquals("12.34", eval("=FormatNumber(12.345)"));
    assertEquals("-12,345.00", eval("=FormatNumber(-12345)"));
    assertEquals("-0.12", eval("=FormatNumber(-0.12345)"));
    assertEquals("-12.34", eval("=FormatNumber(-12.345)"));
    assertEquals("12,345.000", eval("=FormatNumber(12345,3)"));
    assertEquals("0.123", eval("=FormatNumber(0.12345,3)"));
    assertEquals("12.345", eval("=FormatNumber(12.345,3)"));
    assertEquals("12,345", eval("=FormatNumber(12345,0)"));
    assertEquals("0", eval("=FormatNumber(0.12345,0)"));
    assertEquals("12", eval("=FormatNumber(12.345,0)"));
    assertEquals("0.123", eval("=FormatNumber(0.12345,3,True)"));
    assertEquals(".123", eval("=FormatNumber(0.12345,3,False)"));
    assertEquals("-0.123", eval("=FormatNumber(-0.12345,3,True)"));
    assertEquals("-.123", eval("=FormatNumber(-0.12345,3,False)"));
    assertEquals("-12.34", eval("=FormatNumber(-12.345,-1,True,False)"));
    assertEquals("(12.34)", eval("=FormatNumber(-12.345,-1,True,True)"));
    assertEquals("(12)", eval("=FormatNumber(-12.345,0,True,True)"));
    assertEquals("12,345.00", eval("=FormatNumber(12345,-1,-2,-2,True)"));
    assertEquals("12345.00", eval("=FormatNumber(12345,-1,-2,-2,False)"));

    assertEquals("1,234,500.00%", eval("=FormatPercent(12345)"));
    assertEquals("(1,234.50%)", eval("=FormatPercent(-12.345,-1,True,True)"));
    assertEquals("34%", eval("=FormatPercent(0.345,0,True,True)"));
    assertEquals("-.123%", eval("=FormatPercent(-0.0012345,3,False)"));

    assertEquals("$12,345.00", eval("=FormatCurrency(12345)"));
    assertEquals("($12,345.00)", eval("=FormatCurrency(-12345)"));
    assertEquals("-$12.34", eval("=FormatCurrency(-12.345,-1,True,False)"));
    assertEquals("$12", eval("=FormatCurrency(12.345,0,True,True)"));
    assertEquals("($.123)", eval("=FormatCurrency(-0.12345,3,False)"));

    assertEquals("1/1/1973 1:37:25 PM", eval("=FormatDateTime(#1/1/1973 1:37:25 PM#)"));
    assertEquals("1:37:25 PM", eval("=FormatDateTime(#1:37:25 PM#,0)"));
    assertEquals("1/1/1973", eval("=FormatDateTime(#1/1/1973#,0)"));
    assertEquals("Monday, January 01, 1973", eval("=FormatDateTime(#1/1/1973 1:37:25 PM#,1)"));
    assertEquals("1/1/1973", eval("=FormatDateTime(#1/1/1973 1:37:25 PM#,2)"));
    assertEquals("1:37:25 PM", eval("=FormatDateTime(#1/1/1973 1:37:25 PM#,3)"));
    assertEquals("13:37", eval("=FormatDateTime(#1/1/1973 1:37:25 PM#,4)"));
  }

  @Test
  public void testFormatGeneralNumber() throws Exception
  {
    assertEquals(String.format("%.4f", 12345.6789), eval("=Format(12345.6789, 'General Number')"));
    assertEquals(String.format("%.5f", 0.12345), eval("=Format(0.12345, 'General Number')"));
    assertEquals(String.format("%.4f", -12345.6789), eval("=Format(-12345.6789, 'General Number')"));
    assertEquals(String.format("%.5f", -0.12345), eval("=Format(-0.12345, 'General Number')"));
    assertEquals(String.format("%.4f", 12345.6789), eval("=Format('12345.6789', 'General Number')"));
    assertEquals(String.format("%.1f", 1678.9), eval("=Format('1.6789E+3', 'General Number')"));
    assertEquals(String.format("%.10f", 37623.2916666667), eval("=Format(#01/02/2003 7:00:00 AM#, 'General Number')"));
    assertEquals("foo", eval("=Format('foo', 'General Number')"));
  }

  @Test
  public void testFormatStandard() throws Exception
  {
    assertEquals("12,345.68", eval("=Format(12345.6789, 'Standard')"));
    assertEquals("0.12", eval("=Format(0.12345, 'Standard')"));
    assertEquals("-12,345.68", eval("=Format(-12345.6789, 'Standard')"));
    assertEquals("-0.12", eval("=Format(-0.12345, 'Standard')"));
  }

  @Test
  public void testFormatFixed() throws Exception
  {
    assertEquals("12345.68", eval("=Format(12345.6789, 'Fixed')"));
    assertEquals("0.12", eval("=Format(0.12345, 'Fixed')"));
    assertEquals("-12345.68", eval("=Format(-12345.6789, 'Fixed')"));
    assertEquals("-0.12", eval("=Format(-0.12345, 'Fixed')"));
  }

  @Test
  public void testFormatEuro() throws Exception
  {
    assertEquals("\u20AC12,345.68", eval("=Format(12345.6789, 'Euro')"));
    assertEquals("\u20AC0.12", eval("=Format(0.12345, 'Euro')"));
    assertEquals("(\u20AC12,345.68)", eval("=Format(-12345.6789, 'Euro')"));
    assertEquals("(\u20AC0.12)", eval("=Format(-0.12345, 'Euro')"));
  }

  @Test
  public void testFormatCurrency() throws Exception
  {
    assertEquals("$12,345.68", eval("=Format(12345.6789, 'Currency')"));
    assertEquals("$0.12", eval("=Format(0.12345, 'Currency')"));
    assertEquals("($12,345.68)", eval("=Format(-12345.6789, 'Currency')"));
    assertEquals("($0.12)", eval("=Format(-0.12345, 'Currency')"));
  }

  @Test
  public void testFormatPercent() throws Exception
  {
    assertEquals("1234567.89%", eval("=Format(12345.6789, 'Percent')"));
    assertEquals("12.34%", eval("=Format(0.12345, 'Percent')"));
    assertEquals("-1234567.89%", eval("=Format(-12345.6789, 'Percent')"));
    assertEquals("-12.34%", eval("=Format(-0.12345, 'Percent')"));
  }

  @Test
  public void testFormatScientific() throws Exception
  {
    assertEquals("1.23E+4", eval("=Format(12345.6789, 'Scientific')"));
    assertEquals("1.23E-1", eval("=Format(0.12345, 'Scientific')"));
    assertEquals("-1.23E+4", eval("=Format(-12345.6789, 'Scientific')"));
    assertEquals("-1.23E-1", eval("=Format(-0.12345, 'Scientific')"));
  }

  @Test
  public void testFormatBool() throws Exception
  {
    assertEquals("Yes", eval("=Format(True, 'Yes/No')"));
    assertEquals("No", eval("=Format(False, 'Yes/No')"));
    assertEquals("True", eval("=Format(True, 'True/False')"));
    assertEquals("False", eval("=Format(False, 'True/False')"));
    assertEquals("On", eval("=Format(True, 'On/Off')"));
    assertEquals("Off", eval("=Format(False, 'On/Off')"));
  }

  @Test
  public void testFormatDateTime() throws Exception
  {
    assertEquals("1/2/2003 7:00:00 AM", eval("=Format(#01/02/2003 7:00:00 AM#, 'General Date')"));
    assertEquals("1/2/2003", eval("=Format(#01/02/2003#, 'General Date')"));
    assertEquals("7:00:00 AM", eval("=Format(#7:00:00 AM#, 'General Date')"));
    assertEquals("1/2/2003 7:00:00 AM", eval("=Format('37623.2916666667', 'General Date')"));
    assertEquals("foo", eval("=Format('foo', 'General Date')"));
    assertEquals("", eval("=Format('', 'General Date')"));

    assertEquals("Thursday, January 02, 2003", eval("=Format(#01/02/2003 7:00:00 AM#, 'Long Date')"));
    assertEquals("02-Jan-03", eval("=Format(#01/02/2003 7:00:00 AM#, 'Medium Date')"));
    assertEquals("1/2/2003", eval("=Format(#01/02/2003 7:00:00 AM#, 'Short Date')"));
    assertEquals("7:00:00 AM", eval("=Format(#01/02/2003 7:00:00 AM#, 'Long Time')"));
    assertEquals("07:00 AM", eval("=Format(#01/02/2003 7:00:00 AM#, 'Medium Time')"));
    assertEquals("07:00", eval("=Format(#01/02/2003 7:00:00 AM#, 'Short Time')"));
    assertEquals("19:00", eval("=Format(#01/02/2003 7:00:00 PM#, 'Short Time')"));
  }

  @Test
  public void testCustomFormat() throws Exception
  {
    assertEquals("07:00 a", eval("=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p')"));
    assertEquals("07:00 p", eval("=Format(#01/10/2003 7:00:00 PM#, 'hh:nn a/p')"));
    assertEquals("07:00 a 6 2", eval("=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p w ww')"));
    assertEquals("07:00 a 4 1", eval("=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p w ww', 3, 3)"));
    assertEquals("1313", eval("=Format(#01/10/2003 7:13:00 AM#, 'nnnn; foo bar')"));
    assertEquals("1 1/10/2003 7:13:00 AM ttt this is text", eval("=Format(#01/10/2003 7:13:00 AM#, 'q c ttt \"this is text\"')"));
    assertEquals("1 1/10/2003 ttt this is text", eval("=Format(#01/10/2003#, 'q c ttt \"this is text\"')"));
    assertEquals("4 7:13:00 AM ttt this 'is' \"text\"", eval("=Format(#7:13:00 AM#, \"q c ttt \"\"this 'is' \"\"\"\"text\"\"\"\"\"\"\")"));
    assertEquals("12/29/1899", eval("=Format('true', 'c')"));
    assertEquals("Tuesday, 00 Jan 2, 21:36:00 Y", eval("=Format('3.9', '*~dddd, yy mmm d, hh:nn:ss \\Y[Yellow]')"));
    assertEquals("Tuesday, 00 Jan 01/2, 09:36:00 PM", eval("=Format('3.9', 'dddd, yy mmm mm/d, hh:nn:ss AMPM')"));
    assertEquals("9:36:00 PM", eval("=Format('3.9', 'ttttt')"));
    assertEquals("9:36:00 PM", eval("=Format(3.9, 'ttttt')"));
    assertEquals("foo", eval("=Format('foo', 'dddd, yy mmm mm d, hh:nn:ss AMPM')"));

    assertEvalFormat("';\\y;\\n'",
                     "foo", "'foo'",
                     "", "''",
                     "y", "True",
                     "n", "'0'",
                     "", "Null");

    assertEvalFormat("'\\p;\"y\";!\\n;*~\\z[Blue];'",
                     "foo", "'foo'",
                     "", "''",
                     "y", "True",
                     "n", "'0'",
                     "p", "'10'",
                     "z", "Null");

    assertEvalFormat("'\"p\"#.00#\"blah\"'",
                     "p13.00blah", "13",
                     "-p13.00blah", "-13",
                     "p.00blah", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'\"p\"#.00#\"blah\";(\"p\"#.00#\"blah\")'",
                     "p13.00blah", "13",
                     "(p13.00blah)", "-13",
                     "p.00blah", "0",
                     "(p1.00blah)", "True",
                     "p.00blah", "'false'",
                     "p37623.292blah", "#01/02/2003 7:00:00 AM#",
                     "p37623.292blah", "'01/02/2003 7:00:00 AM'",
                     "NotANumber", "'NotANumber'",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'\"p\"#.00#\"blah\";!(\"p\"#.00#\"blah\")[Red];\"zero\"'",
                     "p13.00blah", "13",
                     "(p13.00blah)", "-13",
                     "zero", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'\\p#.00#\"blah\";*~(\"p\"#.00#\"blah\");\"zero\";\"yuck\"'",
                     "p13.00blah", "13",
                     "(p13.00blah)", "-13",
                     "zero", "0",
                     "", "''",
                     "yuck", "Null");

    assertEvalFormat("'0.##;(0.###);\"zero\";\"yuck\";'",
                     "0.03", "0.03",
                     "zero", "0.003",
                     "(0.003)", "-0.003",
                     "zero", "-0.0003");

    assertEvalFormat("'0.##;(0.###E+0)'",
                     "0.03", "0.03",
                     "(3.E-4)", "-0.0003",
                     "0.", "0",
                     "34223.", "34223",
                     "(3.422E+4)", "-34223");

    assertEvalFormat("'0.###E-0'",
                     "3.E-4", "0.0003",
                     "3.422E4", "34223"
                     );

    assertEvalFormat("'0.###e+0'",
                     "3.e-4", "0.0003",
                     "3.422e+4", "34223"
                     );

    assertEvalFormat("'0.###e-0'",
                     "3.e-4", "0.0003",
                     "3.422e4", "34223"
                     );

    assertEvalFormat("'#,##0.###'",
                     "0.003", "0.003",
                     "0.", "0.0003",
                     "34,223.", "34223"
                     );

    assertEvalFormat("'0.'",
                     "13.", "13",
                     "0.", "0.003",
                     "-45.", "-45",
                     "0.", "-0.003",
                     "0.", "0"
                     );

    assertEvalFormat("'0.#'",
                     "13.", "13",
                     "0.3", "0.3",
                     "0.", "0.003",
                     "-45.", "-45",
                     "0.", "-0.003",
                     "0.", "0"
                     );

    assertEvalFormat("'0'",
                     "13", "13",
                     "0", "0.003",
                     "-45", "-45",
                     "0", "-0.003",
                     "0", "0"
                     );

    assertEvalFormat("'%0'",
                     "%13", "0.13",
                     "%0", "0.003",
                     "-%45", "-0.45",
                     "%0", "-0.003",
                     "%0", "0"
                     );

    assertEvalFormat("'#'",
                     "13", "13",
                     "", "0.003",
                     "-45", "-45",
                     "", "-0.003",
                     "", "0"
                     );

    assertEvalFormat("'\\0\\[#.#\\]\\0'",
                     "0[13.]0", "13",
                     "0[.]0", "0.003",
                     "0[.3]0", "0.3",
                     "-0[45.]0", "-45",
                     "0[.]0", "-0.003",
                     "-0[.3]0", "-0.3",
                     "0[.]0", "0"
                     );

    assertEvalFormat("\"#;n'g;'\"",
                     "5", "5",
                     "n'g", "-5",
                     "'", "0");

    assertEvalFormat("'$0.0#'",
                     "$213.0", "213");

    assertEvalFormat("'@'",
                     "foo", "'foo'",
                     "-13", "-13",
                     "0", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'>@'",
                     "FOO", "'foo'",
                     "-13", "-13",
                     "0", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'<@'",
                     "foo", "'FOO'",
                     "-13", "-13",
                     "0", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'!>@;'",
                     "O", "'foo'",
                     "3", "-13",
                     "0", "0",
                     "", "''",
                     "", "Null");

    assertEvalFormat("'!>*~@[Red];\"empty\";'",
                     "O", "'foo'",
                     "3", "-13",
                     "0", "0",
                     "empty", "''",
                     "empty", "Null");

    assertEvalFormat("'><@'",
                     "fOo", "'fOo'");

    assertEvalFormat("'\\x@@@&&&\\y'",
                     "x   fy", "'f'",
                     "x   fooy", "'foo'",
                     "x foobay", "'fooba'",
                     "xfoobarybaz", "'foobarbaz'"
                     );

    assertEvalFormat("'!\\x@@@&&&\\y'",
                     "xf  y", "'f'",
                     "xfooy", "'foo'",
                     "xfoobay", "'fooba'",
                     "xbarbazy", "'foobarbaz'"
                     );

    assertEvalFormat("'\\x&&&@@@\\y'",
                     "x  fy", "'f'",
                     "xfooy", "'foo'",
                     "xfoobay", "'fooba'",
                     "xfoobarybaz", "'foobarbaz'"
                     );

    assertEvalFormat("'!\\x&&&@@@\\y'",
                     "xf   y", "'f'",
                     "xfoo   y", "'foo'",
                     "xfooba y", "'fooba'",
                     "xbarbazy", "'foobarbaz'"
                     );
  }

  private static void assertEvalFormat(String fmtStr, String... testStrs) {
    for(int i = 0; i < testStrs.length; i+=2) {
      String expected = testStrs[i];
      String val = testStrs[i + 1];

      try {
        assertEquals(expected, eval("=Format(" + val + ", " + fmtStr + ")"));
      } catch(AssertionFailedError afe) {
        throw new AssertionFailedError("Input " + val + ": " +
                                       afe.getMessage());
      }
    }
  }

  @Test
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

  @Test
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
    assertEquals(Calendar.getInstance().get(Calendar.YEAR), eval("=Year('01/02 7:00:00 AM')"));

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

    assertEquals(1, eval("=DatePart('ww',#01/03/2018#)"));
    assertEquals(2, eval("=DatePart('ww',#01/03/2018#,4)"));
    assertEquals(1, eval("=DatePart('ww',#01/03/2018#,5)"));
    assertEquals(1, eval("=DatePart('ww',#01/03/2018#,4,3)"));
    assertEquals(52, eval("=DatePart('ww',#01/03/2018#,5,3)"));
    assertEquals(1, eval("=DatePart('ww',#01/03/2018#,4,2)"));
    assertEquals(53, eval("=DatePart('ww',#01/03/2018#,5,2)"));
    assertEquals(2003, eval("=DatePart('yyyy',#11/22/2003 5:45:13 AM#)"));
    assertEquals(4, eval("=DatePart('q',#11/22/2003 5:45:13 AM#)"));
    assertEquals(11, eval("=DatePart('m',#11/22/2003 5:45:13 AM#)"));
    assertEquals(326, eval("=DatePart('y',#11/22/2003 5:45:13 AM#)"));
    assertEquals(22, eval("=DatePart('d',#11/22/2003 5:45:13 AM#)"));
    assertEquals(7, eval("=DatePart('w',#11/22/2003 5:45:13 AM#)"));
    assertEquals(3, eval("=DatePart('w',#11/22/2003 5:45:13 AM#, 5)"));
    assertEquals(5, eval("=DatePart('h',#11/22/2003 5:45:13 AM#)"));
    assertEquals(45, eval("=DatePart('n',#11/22/2003 5:45:13 AM#)"));
    assertEquals(13, eval("=DatePart('s',#11/22/2003 5:45:13 AM#)"));

    assertEquals("11/22/2005 5:45:13 AM", eval("CStr(DateAdd('yyyy',2,#11/22/2003 5:45:13 AM#))"));
    assertEquals("2/22/2004 5:45:13 AM", eval("CStr(DateAdd('q',1,#11/22/2003 5:45:13 AM#))"));
    assertEquals("1/22/2004 5:45:13 AM", eval("CStr(DateAdd('m',2,#11/22/2003 5:45:13 AM#))"));
    assertEquals("12/12/2003 5:45:13 AM", eval("CStr(DateAdd('d',20,#11/22/2003 5:45:13 AM#))"));
    assertEquals("12/12/2003 5:45:13 AM", eval("CStr(DateAdd('w',20,#11/22/2003 5:45:13 AM#))"));
    assertEquals("12/12/2003 5:45:13 AM", eval("CStr(DateAdd('y',20,#11/22/2003 5:45:13 AM#))"));
    assertEquals("12/27/2003 5:45:13 AM", eval("CStr(DateAdd('ww',5,#11/22/2003 5:45:13 AM#))"));
    assertEquals("11/22/2003 3:45:13 PM", eval("CStr(DateAdd('h',10,#11/22/2003 5:45:13 AM#))"));
    assertEquals("11/22/2003 6:19:13 AM", eval("CStr(DateAdd('n',34,#11/22/2003 5:45:13 AM#))"));
    assertEquals("11/22/2003 5:46:27 AM", eval("CStr(DateAdd('s',74,#11/22/2003 5:45:13 AM#))"));

    assertEquals("12/12/2003", eval("CStr(DateAdd('d',20,#11/22/2003#))"));
    assertEquals("11/22/2003 10:00:00 AM", eval("CStr(DateAdd('h',10,#11/22/2003#))"));
    assertEquals("11/23/2003", eval("CStr(DateAdd('h',24,#11/22/2003#))"));
    assertEquals("3:45:13 PM", eval("CStr(DateAdd('h',10,#5:45:13 AM#))"));
    assertEquals("12/31/1899 11:45:13 AM", eval("CStr(DateAdd('h',30,#5:45:13 AM#))"));

    assertEquals(0, eval("=DateDiff('yyyy',#10/22/2003#,#11/22/2003#)"));
    assertEquals(4, eval("=DateDiff('yyyy',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-4, eval("=DateDiff('yyyy',#11/22/2007#,#10/22/2003#)"));

    assertEquals(0, eval("=DateDiff('q',#10/22/2003#,#11/22/2003#)"));
    assertEquals(3, eval("=DateDiff('q',#03/01/2003#,#11/22/2003#)"));
    assertEquals(16, eval("=DateDiff('q',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-13, eval("=DateDiff('q',#03/22/2007#,#10/22/2003#)"));

    assertEquals(1, eval("=DateDiff('m',#10/22/2003#,#11/01/2003#)"));
    assertEquals(8, eval("=DateDiff('m',#03/22/2003#,#11/01/2003#)"));
    assertEquals(49, eval("=DateDiff('m',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-41, eval("=DateDiff('m',#03/22/2007#,#10/01/2003#)"));

    assertEquals(10, eval("=DateDiff('d','10/22','11/01')"));
    assertEquals(0, eval("=DateDiff('y',#1:37:00 AM#,#2:15:00 AM#)"));
    assertEquals(10, eval("=DateDiff('d',#10/22/2003#,#11/01/2003#)"));
    assertEquals(1, eval("=DateDiff('d',#10/22/2003 11:00:00 PM#,#10/23/2003 1:00:00 AM#)"));
    assertEquals(224, eval("=DateDiff('d',#03/22/2003#,#11/01/2003#)"));
    assertEquals(1492, eval("=DateDiff('y',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-1268, eval("=DateDiff('d',#03/22/2007#,#10/01/2003#)"));
    assertEquals(366, eval("=DateDiff('d',#1/1/2000#,#1/1/2001#)"));
    assertEquals(365, eval("=DateDiff('d',#1/1/2001#,#1/1/2002#)"));

    assertEquals(0, eval("=DateDiff('w',#11/3/2018#,#11/04/2018#)"));
    assertEquals(1, eval("=DateDiff('w',#11/3/2018#,#11/10/2018#)"));
    assertEquals(0, eval("=DateDiff('w',#12/31/2017#,#1/1/2018#)"));
    assertEquals(32, eval("=DateDiff('w',#03/22/2003#,#11/01/2003#)"));
    assertEquals(213, eval("=DateDiff('w',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-181, eval("=DateDiff('w',#03/22/2007#,#10/01/2003#)"));

    assertEquals(1, eval("=DateDiff('ww',#11/3/2018#,#11/04/2018#)"));
    assertEquals(1, eval("=DateDiff('ww',#11/3/2018#,#11/10/2018#)"));
    assertEquals(0, eval("=DateDiff('ww',#12/31/2017#,#1/1/2018#)"));
    assertEquals(1, eval("=DateDiff('ww',#12/31/2017#,#1/1/2018#,2)"));
    assertEquals(0, eval("=DateDiff('ww',#12/31/2017#,#1/1/2018#,1,3)"));
    assertEquals(53, eval("=DateDiff('ww',#1/1/2000#,#1/1/2001#)"));
    assertEquals(32, eval("=DateDiff('ww',#03/22/2003#,#11/01/2003#)"));
    assertEquals(213, eval("=DateDiff('ww',#10/22/2003#,#11/22/2007#)"));
    assertEquals(-181, eval("=DateDiff('ww',#03/22/2007#,#10/01/2003#)"));

    assertEquals(1, eval("=DateDiff('h',#1:37:00 AM#,#2:15:00 AM#)"));
    assertEquals(13, eval("=DateDiff('h',#1:37:00 AM#,#2:15:00 PM#)"));
    assertEquals(1, eval("=DateDiff('h',#11/3/2018 1:37:00 AM#,#11/3/2018 2:15:00 AM#)"));
    assertEquals(13, eval("=DateDiff('h',#11/3/2018 1:37:00 AM#,#11/3/2018 2:15:00 PM#)"));
    assertEquals(24, eval("=DateDiff('h',#11/3/2018#,#11/4/2018#)"));
    assertEquals(5641, eval("=DateDiff('h',#3/13/2018 1:37:00 AM#,#11/3/2018 2:15:00 AM#)"));
    assertEquals(23161, eval("=DateDiff('h',#3/13/2016 1:37:00 AM#,#11/3/2018 2:15:00 AM#)"));
    assertEquals(-23173, eval("=DateDiff('h',#11/3/2018 2:15:00 PM#,#3/13/2016 1:37:00 AM#)"));

    assertEquals(1, eval("=DateDiff('n',#1:37:59 AM#,#1:38:00 AM#)"));
    assertEquals(758, eval("=DateDiff('n',#1:37:30 AM#,#2:15:13 PM#)"));
    assertEquals(1, eval("=DateDiff('n',#11/3/2018 1:37:59 AM#,#11/3/2018 1:38:00 AM#)"));
    assertEquals(758, eval("=DateDiff('n',#11/3/2018 1:37:59 AM#,#11/3/2018 2:15:00 PM#)"));
    assertEquals(1440, eval("=DateDiff('n',#11/3/2018#,#11/4/2018#)"));
    assertEquals(338438, eval("=DateDiff('n',#3/13/2018 1:37:59 AM#,#11/3/2018 2:15:00 AM#)"));
    assertEquals(1389638, eval("=DateDiff('n',#3/13/2016 1:37:30 AM#,#11/3/2018 2:15:13 AM#)"));
    assertEquals(-1390358, eval("=DateDiff('n',#11/3/2018 2:15:30 PM#,#3/13/2016 1:37:13 AM#)"));

    assertEquals(1, eval("=DateDiff('s',#1:37:59 AM#,#1:38:00 AM#)"));
    assertEquals(35, eval("=DateDiff('s',#1:37:10 AM#,#1:37:45 AM#)"));
    assertEquals(45463, eval("=DateDiff('s',#1:37:30 AM#,#2:15:13 PM#)"));
    assertEquals(1, eval("=DateDiff('s',#11/3/2018 1:37:59 AM#,#11/3/2018 1:38:00 AM#)"));
    assertEquals(45463, eval("=DateDiff('s',#11/3/2018 1:37:30 AM#,#11/3/2018 2:15:13 PM#)"));
    assertEquals(86400, eval("=DateDiff('s',#11/3/2018#,#11/4/2018#)"));
    assertEquals(20306221, eval("=DateDiff('s',#3/13/2018 1:37:59 AM#,#11/3/2018 2:15:00 AM#)"));
    assertEquals(83378263, eval("=DateDiff('s',#3/13/2016 1:37:30 AM#,#11/3/2018 2:15:13 AM#)"));
    assertEquals(-83421497, eval("=DateDiff('s',#11/3/2018 2:15:30 PM#,#3/13/2016 1:37:13 AM#)"));
  }

  @Test
  public void testFinancialFuncs() throws Exception
  {
    assertEquals(-9.57859403981306, eval("=NPer(0.12/12,-100,-1000)"));
    assertEquals(-9.488095005505778, eval("=NPer(0.12/12,-100,-1000,0,1)"));
    assertEquals(60.0821228537616, eval("=NPer(0.12/12,-100,-1000,10000)"));
    assertEquals(59.67386567429468, eval("=NPer(0.12/12,-100,-1000,10000,1)"));
    assertEquals(69.66071689357466, eval("=NPer(0.12/12,-100,0,10000)"));
    assertEquals(69.16196067980046, eval("=NPer(0.12/12,-100,0,10000,1)"));

    assertEquals(8166.966985640913, eval("=FV(0.12/12,60,-100)"));
    assertEquals(8248.636655497321, eval("=FV(0.12/12,60,-100,0,1)"));
    assertEquals(6350.270287076822, eval("=FV(0.12/12,60,-100,1000)"));
    assertEquals(6431.93995693323, eval("=FV(0.12/12,60,-100,1000,1)"));

    assertEquals(4495.503840622403, eval("=PV(0.12/12,60,-100)"));
    assertEquals(4540.458879028627, eval("=PV(0.12/12,60,-100,0,1)"));
    assertEquals(-1008.9923187551934, eval("=PV(0.12/12,60,-100,10000)"));
    assertEquals(-964.03728034897, eval("=PV(0.12/12,60,-100,10000,1)"));

    assertEquals(22.244447684901765, eval("=Pmt(0.12/12,60,-1000)"));
    assertEquals(22.024205628615608, eval("=Pmt(0.12/12,60,-1000,0,1)"));
    assertEquals(-100.20002916411586, eval("=Pmt(0.12/12,60,-1000,10000)"));
    assertEquals(-99.20794966744144, eval("=Pmt(0.12/12,60,-1000,10000,1)"));
    assertEquals(-122.44447684901762, eval("=Pmt(0.12/12,60,0,10000)"));
    assertEquals(-121.23215529605704, eval("=Pmt(0.12/12,60,0,10000,1)"));
    assertEquals(22.244447684901765, eval("=Pmt(0.12/12,60,-1000)"));

    assertEquals(10d, eval("=IPmt(0.12/12,1,60,-1000)"));
    assertEquals(5.904184782975669, eval("=IPmt(0.12/12,30,60,-1000)"));
    assertEquals(0d, eval("=IPmt(0.12/12,1,60,-1000,0,1)"));
    assertEquals(5.845727507896702, eval("=IPmt(0.12/12,30,60,-1000,0,1)"));
    assertEquals(-0d, eval("=IPmt(0.12/12,1,60,0,10000)"));
    assertEquals(40.958152170243295, eval("=IPmt(0.12/12,30,60,0,10000)"));
    assertEquals(0d, eval("=IPmt(0.12/12,1,60,0,10000,1)"));
    assertEquals(40.55262591113197, eval("=IPmt(0.12/12,30,60,0,10000,1)"));
    assertEquals(10d, eval("=IPmt(0.12/12,1,60,-1000,10000)"));
    assertEquals(46.862336953218964, eval("IPmt(0.12/12,30,60,-1000,10000)"));
    assertEquals(0d, eval("IPmt(0.12/12,1,60,-1000,10000,1)"));
    assertEquals(46.39835341902867, eval("IPmt(0.12/12,30,60,-1000,10000,1)"));

    assertEquals(12.244447684901765, eval("=PPmt(0.12/12,1,60,-1000)"));
    assertEquals(16.340262901926096, eval("=PPmt(0.12/12,30,60,-1000)"));
    assertEquals(22.024205628615608, eval("=PPmt(0.12/12,1,60,-1000,0,1)"));
    assertEquals(16.178478120718907, eval("=PPmt(0.12/12,30,60,-1000,0,1)"));
    assertEquals(-122.44447684901762, eval("=PPmt(0.12/12,1,60,0,10000)"));
    assertEquals(-163.40262901926093, eval("=PPmt(0.12/12,30,60,0,10000)"));
    assertEquals(-121.23215529605704, eval("=PPmt(0.12/12,1,60,0,10000,1)"));
    assertEquals(-161.784781207189, eval("=PPmt(0.12/12,30,60,0,10000,1)"));
    assertEquals(-110.20002916411586, eval("=PPmt(0.12/12,1,60,-1000,10000)"));
    assertEquals(-147.06236611733482, eval("=PPmt(0.12/12,30,60,-1000,10000)"));
    assertEquals(-99.20794966744144, eval("=PPmt(0.12/12,1,60,-1000,10000,1)"));
    assertEquals(-145.60630308647012, eval("=PPmt(0.12/12,30,60,-1000,10000,1)"));

    assertEquals(1.3150684931506849, eval("=DDB(2400,300,10*365,1)"));
    assertEquals(40d, eval("=DDB(2400,300,10*12,1)"));
    assertEquals(480d, eval("=DDB(2400,300,10,1)"));
    assertEquals(22.122547200000184, eval("=DDB(2400,300,10,10)"));
    assertEquals(245.76, (double) eval("=DDB(2400,300,10,4)"), 0.0000001);
    assertEquals(307.2, (double) eval("=DDB(2400,300,10,3)"), 0.0000001);
    assertEquals(480d, eval("=DDB(2400,300,10,0.1)"));
    assertEquals(274.7680330751742, eval("=DDB(2400,300,10,3.5)"));

    assertEquals(2250d, eval("=SLN(30000,7500,10)"));
    assertEquals(1000d, eval("=SLN(10000,5000,5)"));
    assertEquals(1142.857142857143, eval("=SLN(8000,0,7)"));

    assertEquals(4090.909090909091, eval("=SYD(30000,7500,10,1)"));
    assertEquals(409.09090909090907, eval("=SYD(30000,7500,10,10)"));

    assertEquals(-1.630483472667564E-02, eval("=Rate(3,200,-610,0,-20,0.1)"));
    assertEquals(7.701472488201652E-03, eval("=Rate(4*12,-200,8000)"));
    assertEquals(-1.0980298053120467, eval("=Rate(60,93.22,5000,0.1)"));
  }

}
