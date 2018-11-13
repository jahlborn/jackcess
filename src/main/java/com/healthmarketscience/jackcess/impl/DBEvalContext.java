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

package com.healthmarketscience.jackcess.impl;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import javax.script.Bindings;
import javax.script.SimpleBindings;

import com.healthmarketscience.jackcess.expr.EvalConfig;
import com.healthmarketscience.jackcess.expr.FunctionLookup;
import com.healthmarketscience.jackcess.expr.NumericConfig;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.impl.expr.DefaultFunctions;
import com.healthmarketscience.jackcess.impl.expr.Expressionator;
import com.healthmarketscience.jackcess.impl.expr.RandomContext;

/**
 *
 * @author James Ahlborn
 */
public class DBEvalContext implements Expressionator.ParseContext, EvalConfig
{
  private static final int MAX_CACHE_SIZE = 10;

  private final DatabaseImpl _db;
  private FunctionLookup _funcs = DefaultFunctions.LOOKUP;
  private Map<String,SimpleDateFormat> _sdfs;
  private Map<String,DecimalFormat> _dfs;
  private TemporalConfig _temporal = TemporalConfig.US_TEMPORAL_CONFIG;
  private NumericConfig _numeric = NumericConfig.US_NUMERIC_CONFIG;
  private final RandomContext _rndCtx = new RandomContext();
  private Bindings _bindings = new SimpleBindings();

  public DBEvalContext(DatabaseImpl db)
  {
    _db = db;
  }

  protected DatabaseImpl getDatabase() {
    return _db;
  }

  public TemporalConfig getTemporalConfig() {
    return _temporal;
  }

  public void setTemporalConfig(TemporalConfig temporal) {
    _temporal = temporal;
  }

  public Calendar getCalendar() {
    return _db.getCalendar();
  }

  public NumericConfig getNumericConfig() {
    return _numeric;
  }

  public void setNumericConfig(NumericConfig numeric) {
    _numeric = numeric;
  }

  public FunctionLookup getFunctionLookup() {
    return _funcs;
  }

  public void setFunctionLookup(FunctionLookup lookup) {
    _funcs = lookup;
  }

  public Bindings getBindings() {
    return _bindings;
  }

  public void setBindings(Bindings bindings) {
    _bindings = bindings;
  }

  public SimpleDateFormat createDateFormat(String formatStr) {
    if(_sdfs == null) {
      _sdfs = new SimpleCache<String,SimpleDateFormat>(MAX_CACHE_SIZE);
    }
    SimpleDateFormat sdf = _sdfs.get(formatStr);
    if(sdf == null) {
      sdf = _db.createDateFormat(formatStr);
      _sdfs.put(formatStr, sdf);
    }
    return sdf;
  }

  public DecimalFormat createDecimalFormat(String formatStr) {
    if(_dfs == null) {
      _dfs = new SimpleCache<String,DecimalFormat>(MAX_CACHE_SIZE);
    }
    DecimalFormat df = _dfs.get(formatStr);
    if(df == null) {
      df = new DecimalFormat(formatStr, _numeric.getDecimalFormatSymbols());
      _dfs.put(formatStr, df);
    }
    return df;
  }
  
  public float getRandom(Integer seed) {
    return _rndCtx.getRandom(seed);
  }

  void resetDateTimeConfig() {
    _sdfs = null;
  }
}
