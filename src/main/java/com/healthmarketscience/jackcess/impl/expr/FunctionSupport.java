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

import java.util.Arrays;

import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.EvalException;
import com.healthmarketscience.jackcess.expr.Function;
import com.healthmarketscience.jackcess.expr.Value;

/**
 *
 * @author James Ahlborn
 */
public class FunctionSupport
{
  private static final char NON_VAR_SUFFIX = '$';

  private FunctionSupport() {}

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
        throw new EvalException(
            "Invalid number of parameters " +
            num + " passed, expected " + range);
      }
    }

    protected EvalException invalidFunctionCall(
        Throwable t, Value[] params)
    {
      String paramStr = Arrays.toString(params);
      String msg = "Invalid function call {" + _name + "(" +
        paramStr.substring(1, paramStr.length() - 1) + ")}";
      return new EvalException(msg, t);
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }

  public static abstract class Func0 extends BaseFunction
  {
    protected Func0(String name) {
      super(name, 0, 0);
    }

    @Override
    public boolean isPure() {
      // 0-arg functions are usually not pure
      return false;
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval0(ctx);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval0(EvalContext ctx);
  }

  public static abstract class Func1 extends BaseFunction
  {
    protected Func1(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval1(ctx, params[0]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func1NullIsNull extends BaseFunction
  {
    protected Func1NullIsNull(String name) {
      super(name, 1, 1);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        Value param1 = params[0];
        if(param1.isNull()) {
          return param1;
        }
        return eval1(ctx, param1);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval1(EvalContext ctx, Value param);
  }

  public static abstract class Func2 extends BaseFunction
  {
    protected Func2(String name) {
      super(name, 2, 2);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval2(ctx, params[0], params[1]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval2(EvalContext ctx, Value param1, Value param2);
  }

  public static abstract class Func3 extends BaseFunction
  {
    protected Func3(String name) {
      super(name, 3, 3);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return eval3(ctx, params[0], params[1], params[2]);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value eval3(EvalContext ctx,
                                   Value param1, Value param2, Value param3);
  }

  public static abstract class FuncVar extends BaseFunction
  {
    protected FuncVar(String name) {
      super(name, 0, Integer.MAX_VALUE);
    }

    protected FuncVar(String name, int minParams, int maxParams) {
      super(name, minParams, maxParams);
    }

    public final Value eval(EvalContext ctx, Value... params) {
      try {
        validateNumParams(params);
        return evalVar(ctx, params);
      } catch(Exception e) {
        throw invalidFunctionCall(e, params);
      }
    }

    protected abstract Value evalVar(EvalContext ctx, Value[] params);
  }

  public static class StringFuncWrapper implements Function
  {
    private final String _name;
    private final Function _delegate;

    public StringFuncWrapper(Function delegate) {
      _delegate = delegate;
      _name = _delegate.getName() + NON_VAR_SUFFIX;
    }

    public String getName() {
      return _name;
    }

    public boolean isPure() {
      return _delegate.isPure();
    }

    public Value eval(EvalContext ctx, Value... params) {
      Value result = _delegate.eval(ctx, params);
      if(result.isNull()) {
        // non-variant version does not do null-propagation, so force
        // exception to be thrown here
        result.getAsString(ctx);
      }
      return result;
    }

    @Override
    public String toString() {
      return getName() + "()";
    }
  }

}
