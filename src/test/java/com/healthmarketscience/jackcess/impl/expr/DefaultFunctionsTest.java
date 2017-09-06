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
    assertEquals(102L, eval("=Asc(\"foo\")"));
    assertEquals(9786L, eval("=AscW(\"\u263A\")"));
    assertEquals("f", eval("=Chr(102)"));
    assertEquals("\u263A", eval("=ChrW(9786)"));
    assertEquals("263A", eval("=Hex(9786)"));
  }

}
