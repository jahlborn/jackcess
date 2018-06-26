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
import java.util.List;

/**
 *
 * @author James Ahlborn
 */
public abstract class TopoSorter<E>
{
  public static final boolean REVERSE = true;

  // https://en.wikipedia.org/wiki/Topological_sorting
  private static final int UNMARKED = 0;
  private static final int TEMP_MARK = 1;
  private final static int PERM_MARK = 2;

  private final List<E> _values;
  private final List<Node<E>> _nodes = new ArrayList<Node<E>>();
  private final boolean _reverse;

  protected TopoSorter(List<E> values, boolean reverse) {
    _values = values;
    _reverse = reverse;
  }

  public void sort() {
      
    for(E val : _values) {
      Node<E> node = new Node<E>(val);
      getDescendents(val, node._descs);

      // build the internal list in reverse so that we maintain the "original"
      // order of items which we don't need to re-arrange
      _nodes.add(0, node);
    }

    _values.clear();

    for(Node<E> node : _nodes) {
      if(node._mark != UNMARKED) {
        continue;
      }

      visit(node);
    }
  }

  private void visit(Node<E> node) {
    
    if(node._mark == PERM_MARK) {
      return;
    }

    if(node._mark == TEMP_MARK) {
      throw new IllegalStateException("Cycle detected");
    }

    node._mark = TEMP_MARK;

    for(E descVal : node._descs) {
      Node<E> desc = findDescendent(descVal);
      visit(desc);
    }

    node._mark = PERM_MARK;

    if(_reverse) {
      _values.add(node._val);
    } else {
      _values.add(0, node._val);
    }
  }

  private Node<E> findDescendent(E val) {
    for(Node<E> node : _nodes) {
      if(node._val == val) {
        return node;
      }
    }
    throw new IllegalStateException("Unknown descendent " + val);
  }

  protected abstract void getDescendents(E from, List<E> descendents);

  
  private static class Node<E>
  {
    private final E _val;
    private final List<E> _descs = new ArrayList<E>();
    private int _mark = UNMARKED;

    private Node(E val) {
      _val = val;
    }
  }
}
