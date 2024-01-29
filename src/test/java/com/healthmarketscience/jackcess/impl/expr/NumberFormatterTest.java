/*
Copyright (c) 2018 James Ahlborn

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
import java.util.Locale;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 *
 * @author James Ahlborn / Markus Spann
 */
public class NumberFormatterTest
{

  // test ins this class assume a US locale
  private static NumberFormatter nf = new NumberFormatter(Locale.US);
  
  @Test
  public void testDoubleFormat() throws Exception
  {
    assertEquals("894984737284944", nf.formatImpl(894984737284944d));
    assertEquals("-894984737284944", nf.formatImpl(-894984737284944d));
    assertEquals("8949.84737284944", nf.formatImpl(8949.84737284944d));
    assertEquals("8949847372844", nf.formatImpl(8949847372844d));
    assertEquals("8949.847384944", nf.formatImpl(8949.847384944d));
    assertEquals("8.94985647372849E+16", nf.formatImpl(89498564737284944d));
    assertEquals("-8.94985647372849E+16", nf.formatImpl(-89498564737284944d));
    assertEquals("895649.847372849", nf.formatImpl(895649.84737284944d));
    assertEquals("300", nf.formatImpl(300d));
    assertEquals("-300", nf.formatImpl(-300d));
    assertEquals("0.3", nf.formatImpl(0.3d));
    assertEquals("0.1", nf.formatImpl(0.1d));
    assertEquals("2.3423421E-12", nf.formatImpl(0.0000000000023423421d));
    assertEquals("2.3423421E-11", nf.formatImpl(0.000000000023423421d));
    assertEquals("2.3423421E-10", nf.formatImpl(0.00000000023423421d));
    assertEquals("-2.3423421E-10", nf.formatImpl(-0.00000000023423421d));
    assertEquals("2.34234214E-12", nf.formatImpl(0.00000000000234234214d));
    assertEquals("2.342342156E-12", nf.formatImpl(0.000000000002342342156d));
    assertEquals("0.000000023423421", nf.formatImpl(0.000000023423421d));
    assertEquals("2.342342133E-07", nf.formatImpl(0.0000002342342133d));
    assertEquals("1.#INF", nf.formatImpl(Double.POSITIVE_INFINITY));
    assertEquals("-1.#INF", nf.formatImpl(Double.NEGATIVE_INFINITY));
    assertEquals("1.#QNAN", nf.formatImpl(Double.NaN));
  }

  @Test
  public void testFloatFormat() throws Exception
  {
    assertEquals("8949847", nf.formatImpl(8949847f));
    assertEquals("-8949847", nf.formatImpl(-8949847f));
    assertEquals("8949.847", nf.formatImpl(8949.847f));
    assertEquals("894984", nf.formatImpl(894984f));
    assertEquals("8949.84", nf.formatImpl(8949.84f));
    assertEquals("8.949856E+16", nf.formatImpl(89498564737284944f));
    assertEquals("-8.949856E+16", nf.formatImpl(-89498564737284944f));
    assertEquals("895649.9", nf.formatImpl(895649.84737284944f));
    assertEquals("300", nf.formatImpl(300f));
    assertEquals("-300", nf.formatImpl(-300f));
    assertEquals("0.3", nf.formatImpl(0.3f));
    assertEquals("0.1", nf.formatImpl(0.1f));
    assertEquals("2.342342E-12", nf.formatImpl(0.0000000000023423421f));
    assertEquals("2.342342E-11", nf.formatImpl(0.000000000023423421f));
    assertEquals("2.342342E-10", nf.formatImpl(0.00000000023423421f));
    assertEquals("-2.342342E-10", nf.formatImpl(-0.00000000023423421f));
    assertEquals("2.342342E-12", nf.formatImpl(0.00000000000234234214f));
    assertEquals("2.342342E-12", nf.formatImpl(0.000000000002342342156f));
    assertEquals("0.0000234", nf.formatImpl(0.0000234f));
    assertEquals("2.342E-05", nf.formatImpl(0.00002342f));
    assertEquals("1.#INF", nf.formatImpl(Float.POSITIVE_INFINITY));
    assertEquals("-1.#INF", nf.formatImpl(Float.NEGATIVE_INFINITY));
    assertEquals("1.#QNAN", nf.formatImpl(Float.NaN));
  }

  @Test
  public void testDecimalFormat() throws Exception
  {
    assertEquals("9874539485972.2342342234234", nf.formatImpl(new BigDecimal("9874539485972.2342342234234")));
    assertEquals("9874539485972.234234223423468", nf.formatImpl(new BigDecimal("9874539485972.2342342234234678")));
    assertEquals("-9874539485972.234234223423468", nf.formatImpl(new BigDecimal("-9874539485972.2342342234234678")));
    assertEquals("9.874539485972234234223423468E+31", nf.formatImpl(new BigDecimal("98745394859722342342234234678000")));
    assertEquals("9.874539485972234234223423468E+31", nf.formatImpl(new BigDecimal("98745394859722342342234234678000")));
    assertEquals("-9.874539485972234234223423468E+31", nf.formatImpl(new BigDecimal("-98745394859722342342234234678000")));
    assertEquals("300", nf.formatImpl(new BigDecimal("300.0")));
    assertEquals("-300", nf.formatImpl(new BigDecimal("-300.000")));
    assertEquals("0.3", nf.formatImpl(new BigDecimal("0.3")));
    assertEquals("0.1", nf.formatImpl(new BigDecimal("0.1000")));
    assertEquals("0.0000000000023423428930458", nf.formatImpl(new BigDecimal("0.0000000000023423428930458")));
    assertEquals("2.3423428930458389038451E-12", nf.formatImpl(new BigDecimal("0.0000000000023423428930458389038451")));
    assertEquals("2.342342893045838903845134766E-12", nf.formatImpl(new BigDecimal("0.0000000000023423428930458389038451347656")));
  }
}
