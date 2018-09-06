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

/**
 * Jackcess has support for evaluating Access expressions (beta support as of
 * the 2.2.0 release).  This functionality is currently disabled by default
 * but can be globally enabled via the system property
 * "com.healthmarketscience.jackcess.enableExpressionEvaluation" or
 * selectively enabled on a per database basis using {@link com.healthmarketscience.jackcess.Database#setEvaluateExpressions(Boolean)}.
 * <p/>
 * The expression evaluation engine implementation does its best to follow all
 * the warts and idiosyncracies of Access expression evaluation (both those
 * that are documented as well as those discovered through experimentation).
 * These include such things as value conversions, "Null" handling, rounding
 * rules, and implicit interpretations of expression in certain contexts.
 * <p/>
 * Expressions can be used in a number of different places within an Access
 * database.  When enabled, Jackcess supports the following usage:
 * <ul>
 *   <li><b>Default Values:</b> When a row is added which has a
 *       {@code null} value for a field which has a default value
 *       expression defined, that expression will be evaluated and the
 *       result will be inserted for that field.  Like an auto-generated
 *       id, the generated default value will be returned in the input
 *       row array.</li>
 *   <li><b>Calculated Fields:</b> In databases which support calculated
 *       fields, any input value for a calculated field will be ignored.
 *       Instead, the result of evaluating the calculated field
 *       expression will be inserted.  Like an auto-generated id, the
 *       calculated value will be returned in the input row array.</li>
 *   <li><b>Field Validation:</b> Field validation rules will be
 *       evaluated whenever a field value is updated.  If the rule fails,
 *       the update operation will fail.  The failure message will
 *       specify which field's validation rule failed and include the
 *       custom validation rule message if defined.</li>
 *   <li><b>Record Validation:</b> Similar to field validation rules,
 *       record validation rules will be run for the entire record before
 *       update.  Failures are handled in a similar manner.</li>
 * </ul>
 * <p/>
 * <h2>Supporting Classes</h2>
 * <p/>
 * The classes in this package make up the public api for expression handling
 * in Jackcess.  They generally fall into two categories:
 * <p/>
 * <h3>General Use Classes</h3>
 * <p/>
 * <ul>
 * <li>{@link com.healthmarketscience.jackcess.expr.EvalConfig} allows for customization of the expression
 *     evaluation context for a given {@link com.healthmarketscience.jackcess.Database} instance.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.TemporalConfig} encapsulates date/time formatting options for
 *     expression evaluation.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.FunctionLookup} provides a source for {@link com.healthmarketscience.jackcess.expr.Function} instances
 *     used during expression evaluation.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.EvalException} wrapper exception thrown for failures which occur
 *     during expression evaluation.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.ParseException} wrapper exception thrown for failures which
 *     occur during expression parsing.</li>
 * </ul>
 * <p/>
 * <h3>Advanced Use Classes</h3>
 * <p/>
 * <ul>
 * <li>{@link com.healthmarketscience.jackcess.expr.EvalContext} encapsulates all shared state for expression
 *     parsing and evaluation.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.Expression} provides an executable handle to an actual
 *     Access expression.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.Function} provides an invokable handle to external functionality
 *     to an expression.</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.Identifier} identifies a database entity (e.g. the name of a
 *     database field).</li>
 * <li>{@link com.healthmarketscience.jackcess.expr.Value} represents a typed primitive value.</li>
 * </ul>
 * <p/>
 * <h2>Function Support</h2>
 * <p/>
 * Jackcess supports many of the standard Access functions.  The following
 * tables list the (hopefully) current status of support built into Jackcess.
 *
 * <h3>Conversion</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>Asc</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>AscW</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Chr</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>ChrW</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>EuroConvert</td><td></td></tr>
 * <tr class="TableRowColor"><td>FormatCurrency</td><td></td></tr>
 * <tr class="TableRowColor"><td>FormatDateTime</td><td></td></tr>
 * <tr class="TableRowColor"><td>FormatNumber</td><td></td></tr>
 * <tr class="TableRowColor"><td>FormatPercent</td><td></td></tr>
 * <tr class="TableRowColor"><td>GUIDFromString</td><td></td></tr>
 * <tr class="TableRowColor"><td>Hex[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Nz</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Oct[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Str[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>StringFromGUID</td><td></td></tr>
 * <tr class="TableRowColor"><td>Val</td><td></td></tr>
 * <tr class="TableRowColor"><td>CBool</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CByte</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CCur</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CDate</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CVDate</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CDbl</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CDec</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CInt</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CLng</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CSng</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CStr</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>CVar</td><td>Y</td></tr>
 * </table>
 *
 * <h3>Date/Time</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>Day</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Date </td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>DateAdd</td><td></td></tr>
 * <tr class="TableRowColor"><td>DateDiff</td><td></td></tr>
 * <tr class="TableRowColor"><td>DatePart</td><td></td></tr>
 * <tr class="TableRowColor"><td>DateSerial</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>DateValue</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Hour</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Minute</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Month</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>MonthName</td><td></td></tr>
 * <tr class="TableRowColor"><td>Now</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Second</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Time</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Timer</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>TimeSerial</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>TimeValue</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Weekday</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>WeekdayName</td><td></td></tr>
 * <tr class="TableRowColor"><td>Year</td><td>Y</td></tr>
 * </table>
 *
 * <h3>Financial</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>DDB</td><td></td></tr>
 * <tr class="TableRowColor"><td>FV</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>IPmt</td><td></td></tr>
 * <tr class="TableRowColor"><td>IRR</td><td></td></tr>
 * <tr class="TableRowColor"><td>MIRR</td><td></td></tr>
 * <tr class="TableRowColor"><td>NPer</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>NPV</td><td></td></tr>
 * <tr class="TableRowColor"><td>Pmt</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>PPmt</td><td></td></tr>
 * <tr class="TableRowColor"><td>PV</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Rate</td><td></td></tr>
 * <tr class="TableRowColor"><td>SLN</td><td></td></tr>
 * <tr class="TableRowColor"><td>SYD</td><td></td></tr>
 * </table>
 *
 * <h3>Inspection</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>IsDate</td><td>Partial</td></tr>
 * <tr class="TableRowColor"><td>IsEmpty</td><td></td></tr>
 * <tr class="TableRowColor"><td>IsError</td><td></td></tr>
 * <tr class="TableRowColor"><td>IsMissing</td><td></td></tr>
 * <tr class="TableRowColor"><td>IsNull</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>IsNumeric</td><td></td></tr>
 * <tr class="TableRowColor"><td>IsObject</td><td></td></tr>
 * <tr class="TableRowColor"><td>TypeName</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>VarType</td><td>Y</td></tr>
 * </table>
 *
 * <h3>Math</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>Abs</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Atn</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Cos</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Exp</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Int</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Fix</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Log</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Rnd</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Round</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Sgn</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Sin</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Sqr</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Tan</td><td>Y</td></tr>
 * </table>
 *
 * <h3>Program Flow</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>Choose</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>IIf</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Switch</td><td>Y</td></tr>
 * </table>
 *
 * <h3>Text</h3>
 *
 * <table border="1" width="25%" cellpadding="3" cellspacing="0">
 * <tr class="TableHeadingColor" align="left"><th>Function</th><th>Supported</th></tr>
 * <tr class="TableRowColor"><td>Format</td><td></td></tr>
 * <tr class="TableRowColor"><td>InStr</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>InStrRev</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>LCase[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Left[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Len</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>LTrim[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>RTrim[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Trim[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Mid[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Replace</td><td></td></tr>
 * <tr class="TableRowColor"><td>Right[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>Space[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>StrComp</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>StrConv</td><td></td></tr>
 * <tr class="TableRowColor"><td>String[$]</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>StrReverse</td><td>Y</td></tr>
 * <tr class="TableRowColor"><td>UCase[$]</td><td>Y</td></tr>
 * </table>
 *
 *
 *
 */
package com.healthmarketscience.jackcess.expr;
