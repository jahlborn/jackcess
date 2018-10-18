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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This class effectively encapsulates the stateful logic of the "Rnd"
 * function.
 *
 * @author James Ahlborn
 */
public class RandomContext
{
  private Source _defRnd;
  private Map<Integer,Source> _rnds;
  // default to the value access uses for "last val" when none has been
  // returned yet
  private float _lastVal = 1.953125E-02f;

  public RandomContext()
  {
  }

  public float getRandom(Integer seed) {

    if(seed == null) {
      if(_defRnd == null) {
        _defRnd = new SimpleSource(createRandom(System.currentTimeMillis()));
      }
      return _defRnd.get();
    }

    if(_rnds == null) {
      // note, we don't use a SimpleCache here because if we discard a Random
      // instance, that will cause the values to be reset
      _rnds = new HashMap<Integer,Source>();
    }

    Source rnd = _rnds.get(seed);
    if(rnd == null) {

      int seedInt = seed;
      if(seedInt > 0) {
        // normal random with a user specified seed
        rnd = new SimpleSource(createRandom(seedInt));
      } else if(seedInt < 0) {
        // returns the same value every time and resets all randoms
        rnd = new ResetSource(createRandom(seedInt));
      } else {
        // returns the last random value returned
        rnd = new LastValSource();
      }

      _rnds.put(seed, rnd);
    }
    return rnd.get();
  }

  private float setLast(float lastVal) {
    _lastVal = lastVal;
    return lastVal;
  }

  private void reset() {
    if(_rnds != null) {
      _rnds.clear();
    }
  }

  private static Random createRandom(long seed) {
    // TODO, support SecureRandom?
    return new Random(seed);
  }

  private abstract class Source
  {
    public float get() {
      return setLast(getImpl());
    }

    protected abstract float getImpl();
  }

  private class SimpleSource extends Source
  {
    private final Random _rnd;

    private SimpleSource(Random rnd) {
      _rnd = rnd;
    }

    @Override
    protected float getImpl() {
      return _rnd.nextFloat();
    }
  }

  private class ResetSource extends Source
  {
    private final float _val;

    private ResetSource(Random rnd) {
      _val = rnd.nextFloat();
    }

    @Override
    protected float getImpl() {
      reset();
      return _val;
    }
  }

  private class LastValSource extends Source
  {
    private LastValSource() {
    }

    @Override
    protected float getImpl() {
      return _lastVal;
    }
  }
}
