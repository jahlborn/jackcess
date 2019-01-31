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

  public void testFuncs() throws Exception
  {
    assertEval("foo", "=IIf(10 > 1, \"foo\", \"bar\")");
    assertEval("bar", "=IIf(10 < 1, \"foo\", \"bar\")");
    assertEval(102, "=Asc(\"foo\")");
    assertEval(9786, "=AscW(\"\u263A\")");
    assertEval("f", "=Chr(102)");
    assertEval("\u263A", "=ChrW(9786)");
    assertEval("263A", "=Hex(9786)");

    assertEval("blah", "=Nz(\"blah\")");
    assertEval("", "=Nz(Null)");
    assertEval("blah", "=Nz(\"blah\",\"FOO\")");
    assertEval("FOO", "=Nz(Null,\"FOO\")");

    assertEval("23072", "=Oct(9786)");
    assertEval(" 9786", "=Str(9786)");
    assertEval("-42", "=Str(-42)");
    assertEval("-42", "=Str$(-42)");
    assertNull(eval("=Str(Null)"));

    try {
      eval("=Str$(Null)");
      fail("EvalException should have been thrown");
    } catch(EvalException expected) {
      // success
    }

    assertEval(-1, "=CBool(\"1\")");
    assertEval(13, "=CByte(\"13\")");
    assertEval(14, "=CByte(\"13.7\")");
    assertEval(new BigDecimal("57.1235"), "=CCur(\"57.12346\")");
    assertEval(new Double("57.12345"), "=CDbl(\"57.12345\")");
    assertEval(new BigDecimal("57.123456789"), "=CDec(\"57.123456789\")");
    assertEval(513, "=CInt(\"513\")");
    assertEval(514, "=CInt(\"513.7\")");
    assertEval(345513, "=CLng(\"345513\")");
    assertEval(345514, "=CLng(\"345513.7\")");
    assertEquals(new Float("57.12345").doubleValue(),
                 eval("=CSng(\"57.12345\")"));
    assertEval("9786", "=CStr(9786)");
    assertEval("-42", "=CStr(-42)");
    assertEval(LocalDateTime.of(2003,1,2,0,0), "=CDate('01/02/2003')");
    assertEval(LocalDateTime.of(2003,1,2,7,0), "=CDate('01/02/2003 7:00:00 AM')");
    assertEval(LocalDateTime.of(1908,3,31,10,48), "=CDate(3013.45)");


    assertEval(-1, "=IsNull(Null)");
    assertEval(0, "=IsNull(13)");
    assertEval(-1, "=IsDate(#01/02/2003#)");
    assertEval(0, "=IsDate('foo')");
    assertEval(0, "=IsDate('200')");

    assertEval(0, "=IsNumeric(Null)");
    assertEval(0, "=IsNumeric('foo')");
    assertEval(0, "=IsNumeric(#01/02/2003#)");
    assertEval(0, "=IsNumeric('01/02/2003')");
    assertEval(-1, "=IsNumeric(37)");
    assertEval(-1, "=IsNumeric(' 37 ')");
    assertEval(-1, "=IsNumeric(' -37.5e2 ')");
    assertEval(-1, "=IsNumeric(' &H37 ')");
    assertEval(0, "=IsNumeric(' &H37foo ')");
    assertEval(0, "=IsNumeric(' &o39 ')");
    assertEval(-1, "=IsNumeric(' &o36 ')");
    assertEval(0, "=IsNumeric(' &o36.1 ')");

    assertEval(1, "=VarType(Null)");
    assertEval(8, "=VarType('blah')");
    assertEval(7, "=VarType(#01/02/2003#)");
    assertEval(3, "=VarType(42)");
    assertEval(5, "=VarType(CDbl(42))");
    assertEval(14, "=VarType(42.3)");

    assertEval("Null", "=TypeName(Null)");
    assertEval("String", "=TypeName('blah')");
    assertEval("Date", "=TypeName(#01/02/2003#)");
    assertEval("Long", "=TypeName(42)");
    assertEval("Double", "=TypeName(CDbl(42))");
    assertEval("Decimal", "=TypeName(42.3)");

    assertEval(2, "=InStr('AFOOBAR', 'FOO')");
    assertEval(2, "=InStr('AFOOBAR', 'foo')");
    assertEval(2, "=InStr(1, 'AFOOBAR', 'foo')");
    assertEval(0, "=InStr(1, 'AFOOBAR', 'foo', 0)");
    assertEval(2, "=InStr(1, 'AFOOBAR', 'foo', 1)");
    assertEval(2, "=InStr(1, 'AFOOBAR', 'FOO', 0)");
    assertEval(2, "=InStr(2, 'AFOOBAR', 'FOO')");
    assertEval(0, "=InStr(3, 'AFOOBAR', 'FOO')");
    assertEval(0, "=InStr(17, 'AFOOBAR', 'FOO')");
    assertEval(2, "=InStr(1, 'AFOOBARFOOBAR', 'FOO')");
    assertEval(8, "=InStr(3, 'AFOOBARFOOBAR', 'FOO')");
    assertNull(eval("=InStr(3, Null, 'FOO')"));

    assertEval(2, "=InStrRev('AFOOBAR', 'FOO')");
    assertEval(2, "=InStrRev('AFOOBAR', 'foo')");
    assertEval(2, "=InStrRev('AFOOBAR', 'foo', -1)");
    assertEval(0, "=InStrRev('AFOOBAR', 'foo', -1, 0)");
    assertEval(2, "=InStrRev('AFOOBAR', 'foo', -1, 1)");
    assertEval(2, "=InStrRev('AFOOBAR', 'FOO', -1, 0)");
    assertEval(2, "=InStrRev('AFOOBAR', 'FOO', 4)");
    assertEval(0, "=InStrRev('AFOOBAR', 'FOO', 3)");
    assertEval(2, "=InStrRev('AFOOBAR', 'FOO', 17)");
    assertEval(2, "=InStrRev('AFOOBARFOOBAR', 'FOO', 9)");
    assertEval(8, "=InStrRev('AFOOBARFOOBAR', 'FOO', 10)");
    assertNull(eval("=InStrRev(Null, 'FOO', 3)"));

    assertEval("FOOO", "=UCase(\"fOoO\")");
    assertEval("fooo", "=LCase(\"fOoO\")");
    assertEval(" FOO \" BAR ", "=UCase(\" foo \"\" bar \")");

    assertEval("bl", "=Left(\"blah\", 2)");
    assertEval("", "=Left(\"blah\", 0)");
    assertEval("blah", "=Left(\"blah\", 17)");
    assertEval("la", "=Mid(\"blah\", 2, 2)");

    assertEval("ah", "=Right(\"blah\", 2)");
    assertEval("", "=Right(\"blah\", 0)");
    assertEval("blah", "=Right(\"blah\", 17)");

    assertEval("blah  ", "=LTrim(\"  blah  \")");
    assertEval("  blah", "=RTrim(\"  blah  \")");
    assertEval("blah", "=Trim(\"  blah  \")");
    assertEval("   ", "=Space(3)");
    assertEval("ddd", "=String(3,'d')");

    assertEval(1, "=StrComp('FOO', 'bar')");
    assertEval(-1, "=StrComp('bar', 'FOO')");
    assertEval(0, "=StrComp('FOO', 'foo')");
    assertEval(-1, "=StrComp('FOO', 'bar', 0)");
    assertEval(1, "=StrComp('bar', 'FOO', 0)");
    assertEval(-1, "=StrComp('FOO', 'foo', 0)");

    assertEval("FOO", "=StrConv('foo', 1)");
    assertEval("foo", "=StrConv('foo', 2)");
    assertEval("foo", "=StrConv('FOO', 2)");
    assertEval("Foo Bar", "=StrConv('FOO bar', 3)");

    assertEval("halb", "=StrReverse('blah')");

    assertEval("foo", "=Choose(1,'foo','bar','blah')");
    assertEval(null, "=Choose(-1,'foo','bar','blah')");
    assertEval("blah", "=Choose(3,'foo','bar','blah')");

    assertEval(null, "=Switch(False,'foo', False, 'bar', False, 'blah')");
    assertEval("bar", "=Switch(False,'foo', True, 'bar', True, 'blah')");
    assertEval("blah", "=Switch(False,'foo', False, 'bar', True, 'blah')");

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

    assertEval(1615198d, "=Val('    1615 198th Street N.E.')");
    assertEval(-1d, "=Val('  &HFFFFwhatever')");
    assertEval(131071d, "=Val('  &H1FFFFwhatever')");
    assertEval(-1d, "=Val('  &HFFFFFFFFwhatever')");
    assertEval(291d, "=Val('  &H123whatever')");
    assertEval(83d, "=Val('  &O123whatever')");
    assertEval(1.23d, "=Val('  1 2 3 e -2 whatever')");
    assertEval(0d, "=Val('  whatever123 ')");
    assertEval(0d, "=Val('')");

    assertEval("faa", "=Replace('foo','o','a')");
    assertEval("faa", "=Replace('fOo','o','a')");
    assertEval("aa", "=Replace('foo','o','a',2)");
    assertEval("oo", "=Replace('foo','o','a',2,0)");
    assertEval("", "=Replace('foo','o','a',4)");
    assertEval("foo", "=Replace('foo','','a')");
    assertEval("o", "=Replace('foo','','a',3)");
    assertEval("fahhabahhaahha", "=Replace('fooboooo','OO','ahha')");
    assertEval("fahhaboooo", "=Replace('fooboooo','OO','ahha',1,1)");
    assertEval("fooboooo", "=Replace('fooboooo','OO','ahha',1,1,0)");
    assertEval("ahhabahhaahha", "=Replace('fooboooo','OO','ahha',2)");
    assertEval("obahhaahha", "=Replace('fooboooo','OO','ahha',3)");
    assertEval("fb", "=Replace('fooboooo','OO','')");
    assertEval("", "=Replace('','o','a')");
    assertEval("foo", "=Replace('foo','foobar','a')");

    assertEval("12,345.00", "=FormatNumber(12345)");
    assertEval("0.12", "=FormatNumber(0.12345)");
    assertEval("12.34", "=FormatNumber(12.345)");
    assertEval("-12,345.00", "=FormatNumber(-12345)");
    assertEval("-0.12", "=FormatNumber(-0.12345)");
    assertEval("-12.34", "=FormatNumber(-12.345)");
    assertEval("12,345.000", "=FormatNumber(12345,3)");
    assertEval("0.123", "=FormatNumber(0.12345,3)");
    assertEval("12.345", "=FormatNumber(12.345,3)");
    assertEval("12,345", "=FormatNumber(12345,0)");
    assertEval("0", "=FormatNumber(0.12345,0)");
    assertEval("12", "=FormatNumber(12.345,0)");
    assertEval("0.123", "=FormatNumber(0.12345,3,True)");
    assertEval(".123", "=FormatNumber(0.12345,3,False)");
    assertEval("-0.123", "=FormatNumber(-0.12345,3,True)");
    assertEval("-.123", "=FormatNumber(-0.12345,3,False)");
    assertEval("-12.34", "=FormatNumber(-12.345,-1,True,False)");
    assertEval("(12.34)", "=FormatNumber(-12.345,-1,True,True)");
    assertEval("(12)", "=FormatNumber(-12.345,0,True,True)");
    assertEval("12,345.00", "=FormatNumber(12345,-1,-2,-2,True)");
    assertEval("12345.00", "=FormatNumber(12345,-1,-2,-2,False)");

    assertEval("1,234,500.00%", "=FormatPercent(12345)");
    assertEval("(1,234.50%)", "=FormatPercent(-12.345,-1,True,True)");
    assertEval("34%", "=FormatPercent(0.345,0,True,True)");
    assertEval("-.123%", "=FormatPercent(-0.0012345,3,False)");

    assertEval("$12,345.00", "=FormatCurrency(12345)");
    assertEval("($12,345.00)", "=FormatCurrency(-12345)");
    assertEval("-$12.34", "=FormatCurrency(-12.345,-1,True,False)");
    assertEval("$12", "=FormatCurrency(12.345,0,True,True)");
    assertEval("($.123)", "=FormatCurrency(-0.12345,3,False)");

    assertEval("1/1/1973 1:37:25 PM", "=FormatDateTime(#1/1/1973 1:37:25 PM#)");
    assertEval("1:37:25 PM", "=FormatDateTime(#1:37:25 PM#,0)");
    assertEval("1/1/1973", "=FormatDateTime(#1/1/1973#,0)");
    assertEval("Monday, January 01, 1973", "=FormatDateTime(#1/1/1973 1:37:25 PM#,1)");
    assertEval("1/1/1973", "=FormatDateTime(#1/1/1973 1:37:25 PM#,2)");
    assertEval("1:37:25 PM", "=FormatDateTime(#1/1/1973 1:37:25 PM#,3)");
    assertEval("13:37", "=FormatDateTime(#1/1/1973 1:37:25 PM#,4)");
  }

  public void testFormat() throws Exception
  {
    assertEval("12345.6789", "=Format(12345.6789, 'General Number')");
    assertEval("0.12345", "=Format(0.12345, 'General Number')");
    assertEval("-12345.6789", "=Format(-12345.6789, 'General Number')");
    assertEval("-0.12345", "=Format(-0.12345, 'General Number')");
    assertEval("12345.6789", "=Format('12345.6789', 'General Number')");
    assertEval("1678.9", "=Format('1.6789E+3', 'General Number')");
    assertEval("37623.2916666667", "=Format(#01/02/2003 7:00:00 AM#, 'General Number')");
    assertEval("foo", "=Format('foo', 'General Number')");

    assertEval("12,345.68", "=Format(12345.6789, 'Standard')");
    assertEval("0.12", "=Format(0.12345, 'Standard')");
    assertEval("-12,345.68", "=Format(-12345.6789, 'Standard')");
    assertEval("-0.12", "=Format(-0.12345, 'Standard')");

    assertEval("12345.68", "=Format(12345.6789, 'Fixed')");
    assertEval("0.12", "=Format(0.12345, 'Fixed')");
    assertEval("-12345.68", "=Format(-12345.6789, 'Fixed')");
    assertEval("-0.12", "=Format(-0.12345, 'Fixed')");

    assertEval("\u20AC12,345.68", "=Format(12345.6789, 'Euro')");
    assertEval("\u20AC0.12", "=Format(0.12345, 'Euro')");
    assertEval("(\u20AC12,345.68)", "=Format(-12345.6789, 'Euro')");
    assertEval("(\u20AC0.12)", "=Format(-0.12345, 'Euro')");

    assertEval("$12,345.68", "=Format(12345.6789, 'Currency')");
    assertEval("$0.12", "=Format(0.12345, 'Currency')");
    assertEval("($12,345.68)", "=Format(-12345.6789, 'Currency')");
    assertEval("($0.12)", "=Format(-0.12345, 'Currency')");

    assertEval("1234567.89%", "=Format(12345.6789, 'Percent')");
    assertEval("12.34%", "=Format(0.12345, 'Percent')");
    assertEval("-1234567.89%", "=Format(-12345.6789, 'Percent')");
    assertEval("-12.34%", "=Format(-0.12345, 'Percent')");

    assertEval("1.23E+4", "=Format(12345.6789, 'Scientific')");
    assertEval("1.23E-1", "=Format(0.12345, 'Scientific')");
    assertEval("-1.23E+4", "=Format(-12345.6789, 'Scientific')");
    assertEval("-1.23E-1", "=Format(-0.12345, 'Scientific')");

    assertEval("Yes", "=Format(True, 'Yes/No')");
    assertEval("No", "=Format(False, 'Yes/No')");
    assertEval("True", "=Format(True, 'True/False')");
    assertEval("False", "=Format(False, 'True/False')");
    assertEval("On", "=Format(True, 'On/Off')");
    assertEval("Off", "=Format(False, 'On/Off')");

    assertEval("1/2/2003 7:00:00 AM", "=Format(#01/02/2003 7:00:00 AM#, 'General Date')");
    assertEval("1/2/2003", "=Format(#01/02/2003#, 'General Date')");
    assertEval("7:00:00 AM", "=Format(#7:00:00 AM#, 'General Date')");
    assertEval("1/2/2003 7:00:00 AM", "=Format('37623.2916666667', 'General Date')");
    assertEval("foo", "=Format('foo', 'General Date')");
    assertEval("", "=Format('', 'General Date')");

    assertEval("Thursday, January 02, 2003", "=Format(#01/02/2003 7:00:00 AM#, 'Long Date')");
    assertEval("02-Jan-03", "=Format(#01/02/2003 7:00:00 AM#, 'Medium Date')");
    assertEval("1/2/2003", "=Format(#01/02/2003 7:00:00 AM#, 'Short Date')");
    assertEval("7:00:00 AM", "=Format(#01/02/2003 7:00:00 AM#, 'Long Time')");
    assertEval("07:00 AM", "=Format(#01/02/2003 7:00:00 AM#, 'Medium Time')");
    assertEval("07:00", "=Format(#01/02/2003 7:00:00 AM#, 'Short Time')");
    assertEval("19:00", "=Format(#01/02/2003 7:00:00 PM#, 'Short Time')");
  }

  public void testCustomFormat() throws Exception
  {
    assertEval("07:00 a", "=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p')");
    assertEval("07:00 p", "=Format(#01/10/2003 7:00:00 PM#, 'hh:nn a/p')");
    assertEval("07:00 a 6 2", "=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p w ww')");
    assertEval("07:00 a 4 1", "=Format(#01/10/2003 7:00:00 AM#, 'hh:nn a/p w ww', 3, 3)");
    assertEval("1313", "=Format(#01/10/2003 7:13:00 AM#, 'nnnn; foo bar')");
    assertEval("1 1/10/2003 7:13:00 AM ttt this is text",
               "=Format(#01/10/2003 7:13:00 AM#, 'q c ttt \"this is text\"')");
    assertEval("1 1/10/2003 ttt this is text",
               "=Format(#01/10/2003#, 'q c ttt \"this is text\"')");
    assertEval("4 7:13:00 AM ttt this 'is' \"text\"",
               "=Format(#7:13:00 AM#, \"q c ttt \"\"this 'is' \"\"\"\"text\"\"\"\"\"\"\")");
    assertEval("12/29/1899", "=Format('true', 'c')");
    assertEval("Tuesday, 00 Jan 2, 21:36:00 Y",
               "=Format('3.9', '*~dddd, yy mmm d, hh:nn:ss \\Y[Yellow]')");
    assertEval("Tuesday, 00 Jan 01/2, 09:36:00 PM",
               "=Format('3.9', 'dddd, yy mmm mm/d, hh:nn:ss AMPM')");
    assertEval("9:36:00 PM",
               "=Format('3.9', 'ttttt')");
    assertEval("9:36:00 PM",
               "=Format(3.9, 'ttttt')");
    assertEval("foo",
               "=Format('foo', 'dddd, yy mmm mm d, hh:nn:ss AMPM')");

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
                     "0", "0.003",
                     "-45", "-45",
                     "0", "-0.003"
                     // FIXME
                     // "", "0"
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
        assertEval(expected,
                   "=Format(" + val + ", " + fmtStr + ")");
      } catch(AssertionFailedError afe) {
        throw new AssertionFailedError("Input " + val + ": " +
                                       afe.getMessage());
      }
    }
  }

  public void testNumberFuncs() throws Exception
  {
    assertEval(1, "=Abs(1)");
    assertEval(1, "=Abs(-1)");
    assertEval(toBD(1.1), "=Abs(-1.1)");

    assertEval(Math.atan(0.2), "=Atan(0.2)");
    assertEval(Math.sin(0.2), "=Sin(0.2)");
    assertEval(Math.tan(0.2), "=Tan(0.2)");
    assertEval(Math.cos(0.2), "=Cos(0.2)");

    assertEval(Math.exp(0.2), "=Exp(0.2)");
    assertEval(Math.log(0.2), "=Log(0.2)");
    assertEval(Math.sqrt(4.3), "=Sqr(4.3)");

    assertEval(3, "=Fix(3.5)");
    assertEval(4, "=Fix(4)");
    assertEval(-3, "=Fix(-3.5)");
    assertEval(-4, "=Fix(-4)");

    assertEval(1, "=Sgn(3.5)");
    assertEval(1, "=Sgn(4)");
    assertEval(-1, "=Sgn(-3.5)");
    assertEval(-1, "=Sgn(-4)");

    assertEval(3, "=Int(3.5)");
    assertEval(4, "=Int(4)");
    assertEval(-4, "=Int(-3.5)");
    assertEval(-4, "=Int(-4)");

    assertEval(toBD(4), "=Round(3.7)");
    assertEval(4, "=Round(4)");
    assertEval(toBD(-4), "=Round(-3.7)");
    assertEval(-4, "=Round(-4)");

    assertEval(toBD(3.73), "=Round(3.7345, 2)");
    assertEval(4, "=Round(4, 2)");
    assertEval(toBD(-3.73), "=Round(-3.7345, 2)");
    assertEval(-4, "=Round(-4, 2)");
  }

  public void testDateFuncs() throws Exception
  {
    assertEval("1/2/2003", "=CStr(DateValue(#01/02/2003 7:00:00 AM#))");
    assertEval("7:00:00 AM", "=CStr(TimeValue(#01/02/2003 7:00:00 AM#))");

    assertEval("1:10:00 PM", "=CStr(#13:10:00#)");

    assertEval(2003, "=Year(#01/02/2003 7:00:00 AM#)");
    assertEval(1, "=Month(#01/02/2003 7:00:00 AM#)");
    assertEval(2, "=Day(#01/02/2003 7:00:00 AM#)");

    assertEval(2003, "=Year('01/02/2003 7:00:00 AM')");
    assertEval(1899, "=Year(#7:00:00 AM#)");
    assertEquals(Calendar.getInstance().get(Calendar.YEAR),
                 eval("=Year('01/02 7:00:00 AM')"));

    assertEval("January", "=MonthName(1)");
    assertEval("Feb", "=MonthName(2,True)");
    assertEval("March", "=MonthName(3,False)");

    assertEval(7, "=Hour(#01/02/2003 7:10:27 AM#)");
    assertEval(19, "=Hour(#01/02/2003 7:10:27 PM#)");
    assertEval(10, "=Minute(#01/02/2003 7:10:27 AM#)");
    assertEval(27, "=Second(#01/02/2003 7:10:27 AM#)");

    assertEval(7, "=Weekday(#11/22/2003#)");
    assertEval(3, "=Weekday(#11/22/2003#, 5)");
    assertEval(1, "=Weekday(#11/22/2003#, 7)");

    assertEval("Sunday", "=WeekdayName(1)");
    assertEval("Sun", "=WeekdayName(1,True)");
    assertEval("Tuesday", "=WeekdayName(1,False,3)");
    assertEval("Thu", "=WeekdayName(3,True,3)");

    assertTrue(((String)eval("=CStr(Date())"))
                 .matches("\\d{1,2}/\\d{1,2}/\\d{4}"));
    assertTrue(((String)eval("=CStr(Time())"))
               .matches("\\d{1,2}:\\d{2}:\\d{2} (AM|PM)"));

    assertEval("3:57:34 AM", "=CStr(TimeSerial(3,57,34))");
    assertEval("3:57:34 PM", "=CStr(TimeSerial(15,57,34))");
    assertEval("5:45:00 AM", "=CStr(TimeSerial(6,-15,0))");
    assertEval("12:00:00 AM", "=CStr(TimeSerial(0,0,0))");
    assertEval("2:00:00 PM", "=CStr(TimeSerial(-10,0,0))");
    assertEval("6:00:00 AM", "=CStr(TimeSerial(30,0,0))");

    assertEval("2/12/1969", "=CStr(DateSerial(69,2,12))");
    assertEval("2/12/2010", "=CStr(DateSerial(10,2,12))");
    assertEval("7/12/2013", "=CStr(DateSerial(2014,-5,12))");
    assertEval("8/7/2013", "=CStr(DateSerial(2014,-5,38))");

    assertEval(1, "=DatePart('ww',#01/03/2018#)");
    assertEval(2, "=DatePart('ww',#01/03/2018#,4)");
    assertEval(1, "=DatePart('ww',#01/03/2018#,5)");
    assertEval(1, "=DatePart('ww',#01/03/2018#,4,3)");
    assertEval(52, "=DatePart('ww',#01/03/2018#,5,3)");
    assertEval(1, "=DatePart('ww',#01/03/2018#,4,2)");
    assertEval(53, "=DatePart('ww',#01/03/2018#,5,2)");
    assertEval(2003, "=DatePart('yyyy',#11/22/2003 5:45:13 AM#)");
    assertEval(4, "=DatePart('q',#11/22/2003 5:45:13 AM#)");
    assertEval(11, "=DatePart('m',#11/22/2003 5:45:13 AM#)");
    assertEval(326, "=DatePart('y',#11/22/2003 5:45:13 AM#)");
    assertEval(22, "=DatePart('d',#11/22/2003 5:45:13 AM#)");
    assertEval(7, "=DatePart('w',#11/22/2003 5:45:13 AM#)");
    assertEval(3, "=DatePart('w',#11/22/2003 5:45:13 AM#, 5)");
    assertEval(5, "=DatePart('h',#11/22/2003 5:45:13 AM#)");
    assertEval(45, "=DatePart('n',#11/22/2003 5:45:13 AM#)");
    assertEval(13, "=DatePart('s',#11/22/2003 5:45:13 AM#)");

    assertEval("11/22/2005 5:45:13 AM", "CStr(DateAdd('yyyy',2,#11/22/2003 5:45:13 AM#))");
    assertEval("2/22/2004 5:45:13 AM", "CStr(DateAdd('q',1,#11/22/2003 5:45:13 AM#))");
    assertEval("1/22/2004 5:45:13 AM", "CStr(DateAdd('m',2,#11/22/2003 5:45:13 AM#))");
    assertEval("12/12/2003 5:45:13 AM", "CStr(DateAdd('d',20,#11/22/2003 5:45:13 AM#))");
    assertEval("12/12/2003 5:45:13 AM", "CStr(DateAdd('w',20,#11/22/2003 5:45:13 AM#))");
    assertEval("12/12/2003 5:45:13 AM", "CStr(DateAdd('y',20,#11/22/2003 5:45:13 AM#))");
    assertEval("12/27/2003 5:45:13 AM", "CStr(DateAdd('ww',5,#11/22/2003 5:45:13 AM#))");
    assertEval("11/22/2003 3:45:13 PM", "CStr(DateAdd('h',10,#11/22/2003 5:45:13 AM#))");
    assertEval("11/22/2003 6:19:13 AM", "CStr(DateAdd('n',34,#11/22/2003 5:45:13 AM#))");
    assertEval("11/22/2003 5:46:27 AM", "CStr(DateAdd('s',74,#11/22/2003 5:45:13 AM#))");

    assertEval("12/12/2003", "CStr(DateAdd('d',20,#11/22/2003#))");
    assertEval("11/22/2003 10:00:00 AM", "CStr(DateAdd('h',10,#11/22/2003#))");
    assertEval("11/23/2003", "CStr(DateAdd('h',24,#11/22/2003#))");
    assertEval("3:45:13 PM", "CStr(DateAdd('h',10,#5:45:13 AM#))");
    assertEval("12/31/1899 11:45:13 AM", "CStr(DateAdd('h',30,#5:45:13 AM#))");

    assertEval(0, "=DateDiff('yyyy',#10/22/2003#,#11/22/2003#)");
    assertEval(4, "=DateDiff('yyyy',#10/22/2003#,#11/22/2007#)");
    assertEval(-4, "=DateDiff('yyyy',#11/22/2007#,#10/22/2003#)");

    assertEval(0, "=DateDiff('q',#10/22/2003#,#11/22/2003#)");
    assertEval(3, "=DateDiff('q',#03/01/2003#,#11/22/2003#)");
    assertEval(16, "=DateDiff('q',#10/22/2003#,#11/22/2007#)");
    assertEval(-13, "=DateDiff('q',#03/22/2007#,#10/22/2003#)");

    assertEval(1, "=DateDiff('m',#10/22/2003#,#11/01/2003#)");
    assertEval(8, "=DateDiff('m',#03/22/2003#,#11/01/2003#)");
    assertEval(49, "=DateDiff('m',#10/22/2003#,#11/22/2007#)");
    assertEval(-41, "=DateDiff('m',#03/22/2007#,#10/01/2003#)");

    assertEval(10, "=DateDiff('d','10/22','11/01')");
    assertEval(0, "=DateDiff('y',#1:37:00 AM#,#2:15:00 AM#)");
    assertEval(10, "=DateDiff('d',#10/22/2003#,#11/01/2003#)");
    assertEval(1, "=DateDiff('d',#10/22/2003 11:00:00 PM#,#10/23/2003 1:00:00 AM#)");
    assertEval(224, "=DateDiff('d',#03/22/2003#,#11/01/2003#)");
    assertEval(1492, "=DateDiff('y',#10/22/2003#,#11/22/2007#)");
    assertEval(-1268, "=DateDiff('d',#03/22/2007#,#10/01/2003#)");
    assertEval(366, "=DateDiff('d',#1/1/2000#,#1/1/2001#)");
    assertEval(365, "=DateDiff('d',#1/1/2001#,#1/1/2002#)");

    assertEval(0, "=DateDiff('w',#11/3/2018#,#11/04/2018#)");
    assertEval(1, "=DateDiff('w',#11/3/2018#,#11/10/2018#)");
    assertEval(0, "=DateDiff('w',#12/31/2017#,#1/1/2018#)");
    assertEval(32, "=DateDiff('w',#03/22/2003#,#11/01/2003#)");
    assertEval(213, "=DateDiff('w',#10/22/2003#,#11/22/2007#)");
    assertEval(-181, "=DateDiff('w',#03/22/2007#,#10/01/2003#)");

    assertEval(1, "=DateDiff('ww',#11/3/2018#,#11/04/2018#)");
    assertEval(1, "=DateDiff('ww',#11/3/2018#,#11/10/2018#)");
    assertEval(0, "=DateDiff('ww',#12/31/2017#,#1/1/2018#)");
    assertEval(1, "=DateDiff('ww',#12/31/2017#,#1/1/2018#,2)");
    assertEval(0, "=DateDiff('ww',#12/31/2017#,#1/1/2018#,1,3)");
    assertEval(53, "=DateDiff('ww',#1/1/2000#,#1/1/2001#)");
    assertEval(32, "=DateDiff('ww',#03/22/2003#,#11/01/2003#)");
    assertEval(213, "=DateDiff('ww',#10/22/2003#,#11/22/2007#)");
    assertEval(-181, "=DateDiff('ww',#03/22/2007#,#10/01/2003#)");

    assertEval(1, "=DateDiff('h',#1:37:00 AM#,#2:15:00 AM#)");
    assertEval(13, "=DateDiff('h',#1:37:00 AM#,#2:15:00 PM#)");
    assertEval(1, "=DateDiff('h',#11/3/2018 1:37:00 AM#,#11/3/2018 2:15:00 AM#)");
    assertEval(13, "=DateDiff('h',#11/3/2018 1:37:00 AM#,#11/3/2018 2:15:00 PM#)");
    assertEval(24, "=DateDiff('h',#11/3/2018#,#11/4/2018#)");
    assertEval(5641, "=DateDiff('h',#3/13/2018 1:37:00 AM#,#11/3/2018 2:15:00 AM#)");
    assertEval(23161, "=DateDiff('h',#3/13/2016 1:37:00 AM#,#11/3/2018 2:15:00 AM#)");
    assertEval(-23173, "=DateDiff('h',#11/3/2018 2:15:00 PM#,#3/13/2016 1:37:00 AM#)");

    assertEval(1, "=DateDiff('n',#1:37:59 AM#,#1:38:00 AM#)");
    assertEval(758, "=DateDiff('n',#1:37:30 AM#,#2:15:13 PM#)");
    assertEval(1, "=DateDiff('n',#11/3/2018 1:37:59 AM#,#11/3/2018 1:38:00 AM#)");
    assertEval(758, "=DateDiff('n',#11/3/2018 1:37:59 AM#,#11/3/2018 2:15:00 PM#)");
    assertEval(1440, "=DateDiff('n',#11/3/2018#,#11/4/2018#)");
    assertEval(338438, "=DateDiff('n',#3/13/2018 1:37:59 AM#,#11/3/2018 2:15:00 AM#)");
    assertEval(1389638, "=DateDiff('n',#3/13/2016 1:37:30 AM#,#11/3/2018 2:15:13 AM#)");
    assertEval(-1390358, "=DateDiff('n',#11/3/2018 2:15:30 PM#,#3/13/2016 1:37:13 AM#)");

    assertEval(1, "=DateDiff('s',#1:37:59 AM#,#1:38:00 AM#)");
    assertEval(35, "=DateDiff('s',#1:37:10 AM#,#1:37:45 AM#)");
    assertEval(45463, "=DateDiff('s',#1:37:30 AM#,#2:15:13 PM#)");
    assertEval(1, "=DateDiff('s',#11/3/2018 1:37:59 AM#,#11/3/2018 1:38:00 AM#)");
    assertEval(45463, "=DateDiff('s',#11/3/2018 1:37:30 AM#,#11/3/2018 2:15:13 PM#)");
    assertEval(86400, "=DateDiff('s',#11/3/2018#,#11/4/2018#)");
    assertEval(20306221, "=DateDiff('s',#3/13/2018 1:37:59 AM#,#11/3/2018 2:15:00 AM#)");
    assertEval(83378263, "=DateDiff('s',#3/13/2016 1:37:30 AM#,#11/3/2018 2:15:13 AM#)");
    assertEval(-83421497, "=DateDiff('s',#11/3/2018 2:15:30 PM#,#3/13/2016 1:37:13 AM#)");
  }

  public void testFinancialFuncs() throws Exception
  {
    assertEval("-9.57859403981306", "=CStr(NPer(0.12/12,-100,-1000))");
    assertEval("-9.48809500550578", "=CStr(NPer(0.12/12,-100,-1000,0,1))");
    assertEval("60.0821228537616", "=CStr(NPer(0.12/12,-100,-1000,10000))");
    assertEval("59.6738656742947", "=CStr(NPer(0.12/12,-100,-1000,10000,1))");
    assertEval("69.6607168935747", "=CStr(NPer(0.12/12,-100,0,10000))");
    assertEval("69.1619606798005", "=CStr(NPer(0.12/12,-100,0,10000,1))");

    assertEval("8166.96698564091", "=CStr(FV(0.12/12,60,-100))");
    assertEval("8248.63665549732", "=CStr(FV(0.12/12,60,-100,0,1))");
    assertEval("6350.27028707682", "=CStr(FV(0.12/12,60,-100,1000))");
    assertEval("6431.93995693323", "=CStr(FV(0.12/12,60,-100,1000,1))");

    assertEval("4495.5038406224", "=CStr(PV(0.12/12,60,-100))");
    assertEval("4540.45887902863", "=CStr(PV(0.12/12,60,-100,0,1))");
    assertEval("-1008.99231875519", "=CStr(PV(0.12/12,60,-100,10000))");
    assertEval("-964.03728034897", "=CStr(PV(0.12/12,60,-100,10000,1))");

    assertEval("22.2444476849018", "=CStr(Pmt(0.12/12,60,-1000))");
    assertEval("22.0242056286156", "=CStr(Pmt(0.12/12,60,-1000,0,1))");
    assertEval("-100.200029164116", "=CStr(Pmt(0.12/12,60,-1000,10000))");
    assertEval("-99.2079496674414", "=CStr(Pmt(0.12/12,60,-1000,10000,1))");
    assertEval("-122.444476849018", "=CStr(Pmt(0.12/12,60,0,10000))");
    assertEval("-121.232155296057", "=CStr(Pmt(0.12/12,60,0,10000,1))");
    assertEval("22.2444476849018", "=CStr(Pmt(0.12/12,60,-1000))");

    assertEval("10", "=CStr(IPmt(0.12/12,1,60,-1000))");
    assertEval("5.90418478297567", "=CStr(IPmt(0.12/12,30,60,-1000))");
    assertEval("0", "=CStr(IPmt(0.12/12,1,60,-1000,0,1))");
    assertEval("5.8457275078967", "=CStr(IPmt(0.12/12,30,60,-1000,0,1))");
    assertEval("0", "=CStr(IPmt(0.12/12,1,60,0,10000))");
    assertEval("40.9581521702433", "=CStr(IPmt(0.12/12,30,60,0,10000))");
    assertEval("0", "=CStr(IPmt(0.12/12,1,60,0,10000,1))");
    assertEval("40.552625911132", "=CStr(IPmt(0.12/12,30,60,0,10000,1))");
    assertEval("10", "=CStr(IPmt(0.12/12,1,60,-1000,10000))");
    assertEval("46.862336953219", "=CStr(IPmt(0.12/12,30,60,-1000,10000))");
    assertEval("0", "=CStr(IPmt(0.12/12,1,60,-1000,10000,1))");
    assertEval("46.3983534190287", "=CStr(IPmt(0.12/12,30,60,-1000,10000,1))");

    assertEval("12.2444476849018", "=CStr(PPmt(0.12/12,1,60,-1000))");
    assertEval("16.3402629019261", "=CStr(PPmt(0.12/12,30,60,-1000))");
    assertEval("22.0242056286156", "=CStr(PPmt(0.12/12,1,60,-1000,0,1))");
    assertEval("16.1784781207189", "=CStr(PPmt(0.12/12,30,60,-1000,0,1))");
    assertEval("-122.444476849018", "=CStr(PPmt(0.12/12,1,60,0,10000))");
    assertEval("-163.402629019261", "=CStr(PPmt(0.12/12,30,60,0,10000))");
    assertEval("-121.232155296057", "=CStr(PPmt(0.12/12,1,60,0,10000,1))");
    assertEval("-161.784781207189", "=CStr(PPmt(0.12/12,30,60,0,10000,1))");
    assertEval("-110.200029164116", "=CStr(PPmt(0.12/12,1,60,-1000,10000))");
    assertEval("-147.062366117335", "=CStr(PPmt(0.12/12,30,60,-1000,10000))");
    assertEval("-99.2079496674414", "=CStr(PPmt(0.12/12,1,60,-1000,10000,1))");
    assertEval("-145.60630308647", "=CStr(PPmt(0.12/12,30,60,-1000,10000,1))");

    assertEval("1.31506849315068", "=CStr(DDB(2400,300,10*365,1))");
    assertEval("40", "=CStr(DDB(2400,300,10*12,1))");
    assertEval("480", "=CStr(DDB(2400,300,10,1))");
    assertEval("22.1225472000002", "=CStr(DDB(2400,300,10,10))");
    assertEval("245.76", "=CStr(DDB(2400,300,10,4))");
    assertEval("307.2", "=CStr(DDB(2400,300,10,3))");
    assertEval("480", "=CStr(DDB(2400,300,10,0.1))");
    assertEval("274.768033075174", "=CStr(DDB(2400,300,10,3.5))");

    assertEval("2250", "=CStr(SLN(30000,7500,10))");
    assertEval("1000", "=CStr(SLN(10000,5000,5))");
    assertEval("1142.85714285714", "=CStr(SLN(8000,0,7))");

    assertEval("4090.90909090909", "=CStr(SYD(30000,7500,10,1))");
    assertEval("409.090909090909", "=CStr(SYD(30000,7500,10,10))");

    assertEval("-1.63048347266756E-02", "=CStr(Rate(3,200,-610,0,-20,0.1))");
    assertEval("7.70147248820165E-03", "=CStr(Rate(4*12,-200,8000))");
    assertEval("-1.09802980531205", "=CStr(Rate(60,93.22,5000,0.1))");
  }

  static void assertEval(Object expected, String exprStr) {
    try {
      assertEquals(expected, eval(exprStr));
    } catch(Error e) {
      // Convenience for adding new tests
      // System.err.println("[ERROR] " + e);
      throw e;
    }
  }
}
