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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class TopoSorterTest extends TestCase
{

  public TopoSorterTest(String name) {
    super(name);
  }

  public void testTopoSort() throws Exception
  {
    doTopoTest(Arrays.asList("A", "B", "C"),
               Arrays.asList("A", "B", "C"));

    doTopoTest(Arrays.asList("B", "A", "C"),
               Arrays.asList("A", "B", "C"),
               "B", "C",
               "A", "B");

    try {
      doTopoTest(Arrays.asList("B", "A", "C"),
                 Arrays.asList("C", "B", "A"),
                 "B", "C",
                 "A", "B",
                 "C", "A");
      fail("IllegalStateException should have been thrown");
    } catch(IllegalStateException expected) {
      // success
      assertTrue(expected.getMessage().startsWith("Cycle"));
    }

    try {
      doTopoTest(Arrays.asList("B", "A", "C"),
                 Arrays.asList("C", "B", "A"),
                 "B", "D");
      fail("IllegalStateException should have been thrown");
    } catch(IllegalStateException expected) {
      // success
      assertTrue(expected.getMessage().startsWith("Unknown descendent"));
    }

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("D", "A", "B", "C"),
               "B", "C",
               "A", "B");

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("A", "D", "B", "C"),
               "B", "C",
               "A", "B",
               "A", "D");

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("D", "A", "C", "B"),
               "D", "A",
               "C", "B");

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("D", "C", "A", "B"),
               "D", "A",
               "C", "B",
               "C", "A");

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("C", "D", "A", "B"),
               "D", "A",
               "C", "B",
               "C", "D");

    doTopoTest(Arrays.asList("B", "D", "A", "C"),
               Arrays.asList("D", "A", "C", "B"),
               "D", "A",
               "C", "B",
               "D", "B");
  }

  private static void doTopoTest(List<String> original,
                                 List<String> expected,
                                 String... descs) {

    List<String> values = new ArrayList<String>();
    values.addAll(original);

    TestTopoSorter tsorter = new TestTopoSorter(values, false);
    for(int i = 0; i < descs.length; i+=2) {
      tsorter.addDescendents(descs[i], descs[i+1]);
    }

    tsorter.sort();

    assertEquals(expected, values);


    values = new ArrayList<String>();
    values.addAll(original);

    tsorter = new TestTopoSorter(values, true);
    for(int i = 0; i < descs.length; i+=2) {
      tsorter.addDescendents(descs[i], descs[i+1]);
    }

    tsorter.sort();

    List<String> expectedReverse = new ArrayList<String>(expected);
    Collections.reverse(expectedReverse);

    assertEquals(expectedReverse, values);
  }

  private static class TestTopoSorter extends TopoSorter<String>
  {
    private final Map<String,List<String>> _descMap = 
      new HashMap<String,List<String>>();

    protected TestTopoSorter(List<String> values, boolean reverse) {
      super(values, reverse);
    }

    public void addDescendents(String from, String... tos) {
      List<String> descs = _descMap.get(from);
      if(descs == null) {
        descs = new ArrayList<String>();
        _descMap.put(from, descs);
      }

      descs.addAll(Arrays.asList(tos));
    }

    @Override
    protected void getDescendents(String from, List<String> descendents) {
      List<String> descs = _descMap.get(from);
      if(descs != null) {
        descendents.addAll(descs);
      }
    }
  }
}
