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

import java.text.SimpleDateFormat;
import java.util.Map;

import com.healthmarketscience.jackcess.expr.EvalConfig;
import com.healthmarketscience.jackcess.expr.Function;
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
  private Map<String,SimpleDateFormat> _sdfs;
  private TemporalConfig _temporal;
  private final RandomContext _rndCtx = new RandomContext();

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

  public void putCustomExpressionFunction(Function func) {
    // FIXME writeme
  }

  public Function getCustomExpressionFunction(String name) {
    // FIXME writeme
    return null;
  }

  public SimpleDateFormat createDateFormat(String formatStr) {
    if(_sdfs == null) {
      _sdfs = new SimpleCache<String,SimpleDateFormat>(MAX_CACHE_SIZE);
    }
    SimpleDateFormat sdf = _sdfs.get(formatStr);
    if(formatStr == null) {
      sdf = _db.createDateFormat(formatStr);
      _sdfs.put(formatStr, sdf);
    }
    return sdf;
  }

  public float getRandom(Integer seed) {
    return _rndCtx.getRandom(seed);
  }

  public Function getExpressionFunction(String name) {
    // FIXME, support custom function context?
    return DefaultFunctions.getFunction(name);
  }
}
