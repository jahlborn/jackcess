/*
Copyright (c) 2019 James Ahlborn

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

package com.healthmarketscience.jackcess.util;

import java.io.IOException;
import java.util.Map;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.expr.EvalConfig;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.impl.ColEvalContext;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.expr.FormatUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility for applying Column formatting to column values for display.  This
 * utility loads the "Format" property from the given column and builds an
 * appropriate formatter (essentially leveraging the internals of the
 * expression execution engine's support for the "Format()" function).  Since
 * formats leverage the expression evaluation engine, the underlying
 * Database's {@link EvalConfig} can be used to alter how this utility formats
 * values.  Note, formatted values may be suitable for <i>display only</i>
 * (i.e. a formatted value may not be accepted as an input value to a Table
 * add/update method).
 *
 * @author James Ahlborn
 * @usage _general_class_
 */
public class ColumnFormatter
{
  private final ColumnImpl _col;
  private final FormatEvalContext _ctx;
  private String _fmtStr;
  private FormatUtil.StandaloneFormatter _fmt;

  public ColumnFormatter(Column col) throws IOException {
    _col = (ColumnImpl)col;
    _ctx = new FormatEvalContext(_col);
    reload();
  }

  /**
   * Returns the currently loaded "Format" property for this formatter, may be
   * {@code null}.
   */
  public String getFormatString() {
    return _fmtStr;
  }

  /**
   * Sets the given format string as the "Format" property for the underlying
   * Column and reloads this formatter.
   *
   * @param fmtStr the new format string.  may be {@code null}, in which case
   *               the "Format" property is removed from the underlying Column
   */
  public void setFormatString(String fmtStr) throws IOException {
    PropertyMap props = _col.getProperties();
    if(!StringUtils.isEmpty(fmtStr)) {
      props.put(PropertyMap.FORMAT_PROP, fmtStr);
    } else {
      props.remove(PropertyMap.FORMAT_PROP);
    }
    props.save();
    reload();
  }

  /**
   * Formats the given value according to the format currently defined for the
   * underlying Column.
   *
   * @param val a valid input value for the DataType of the underlying Column
   *            (i.e. a value which could be passed to a Table add/update
   *            method for this Column).  may be {@code null}
   *
   * @return the formatted result, always non-{@code null}
   */
  public String format(Object val) {
    return _ctx.format(val);
  }

  /**
   * Convenience method for retrieving the appropriate Column value from the
   * given row array and formatting it.
   *
   * @return the formatted result, always non-{@code null}
   */
  public String getRowValue(Object[] rowArray) {
    return format(_col.getRowValue(rowArray));
  }

  /**
   * Convenience method for retrieving the appropriate Column value from the
   * given row map and formatting it.
   *
   * @return the formatted result, always non-{@code null}
   */
  public String getRowValue(Map<String,?> rowMap) {
    return format(_col.getRowValue(rowMap));
  }

  /**
   * If the properties for the underlying Column have been modified directly
   * (or the EvalConfig for the underlying Database has been modified), this
   * method may be called to reload the format for the underlying Column.
   */
  public final void reload() throws IOException {
    _fmt = null;
    _fmtStr = null;

    _fmtStr = (String)_col.getProperties().getValue(PropertyMap.FORMAT_PROP);
    _fmt = FormatUtil.createStandaloneFormatter(_ctx, _fmtStr, 1, 1);
  }

  /**
   * Utility class to provide an EvalContext for the expression evaluation
   * engine format support.
   */
  private class FormatEvalContext extends ColEvalContext
  {
    private FormatEvalContext(ColumnImpl col) {
      super(col);
    }

    public String format(Object val) {
      try {
        return _fmt.format(toValue(val)).getAsString(this);
      } catch(EvalException ee) {
        // invalid values for a given format result in returning the value as is
        return val.toString();
      }
    }
  }
}
