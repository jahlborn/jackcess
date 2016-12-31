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

import java.util.HashMap;
import java.util.Map;


import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.EvalContext;

/**
 *
 * @author James Ahlborn
 */
public class DefaultFunctions 
{
  private static final Map<String,Function> FUNCS = 
    new HashMap<String,Function>();

  private DefaultFunctions() {}

  public static Function getFunction(String name) {
    return FUNCS.get(name.toLowerCase());
  }

  public static abstract class BaseFunction implements Function
  {
    private final String _name;
    private final int _minParams;
    private final int _maxParams;

    protected BaseFunction(String name, int minParams, int maxParams)
    {
      _name = name;
      _minParams = minParams;
      _maxParams = maxParams;
    }

    public String getName() {
      return _name;
    }

    public boolean isPure() {
      // most functions are probably pure, so make this the default
      return true;
    }

    protected void validateNumParams(Value[] params) {
      int num = params.length;
      if((num < _minParams) || (num > _maxParams)) {
        String range = ((_minParams == _maxParams) ? "" + _minParams :
                        _minParams + " to " + _maxParams);
        throw new IllegalArgumentException(
            this + ": invalid number of parameters " +
            num + " passed, expected " + range);
      }
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }

  public static abstract class Func1 extends BaseFunction
  {
    protected Func1(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval1(ctx, params[0]);
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func2 extends BaseFunction
  {
    protected Func2(String name) {
      super(name, 2, 2);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval2(ctx, params[0], params[1]);
    }

    protected abstract Value eval2(EvalContext ctx, Value param1, Value param2);
  }

  public static abstract class Func3 extends BaseFunction
  {
    protected Func3(String name) {
      super(name, 3, 3);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      validateNumParams(params);
      return eval3(ctx, params[0], params[1], params[2]);
    }

    protected abstract Value eval3(EvalContext ctx, 
                                   Value param1, Value param2, Value param3);
  }

  public static final Function IIF = registerFunc(new Func3("IIf") {
    @Override
    protected Value eval3(EvalContext ctx, 
                          Value param1, Value param2, Value param3) {
      // null is false
      return ((!param1.isNull() && param1.getAsBoolean()) ? param2 : param3);
    }
  });

  

  // https://www.techonthenet.com/access/functions/
  // https://support.office.com/en-us/article/Access-Functions-by-category-b8b136c3-2716-4d39-94a2-658ce330ed83

  private static Function registerFunc(Function func) {
    if(FUNCS.put(func.getName().toLowerCase(), func) != null) {
      throw new IllegalStateException("Duplicate function " + func);
    }
    return func;
  }
}
