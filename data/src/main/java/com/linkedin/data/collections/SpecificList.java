/*
   Copyright (c) 2020 LinkedIn Corp.

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

package com.linkedin.data.collections;

import com.linkedin.data.Data;
import com.linkedin.data.DataComplex;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;


/**
 * A list implementation optimized for a particular data schema.
 */
public abstract class SpecificList extends AbstractList<Object> implements Cloneable
{
  public abstract void traverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException;

  /**
   * Returns a shallow copy of this instance: the keys and values themselves are not cloned.
   *
   * @return a shallow copy of this map
   */
  @Override
  public Object clone() throws CloneNotSupportedException
  {
    return super.clone();
  }

  /**
   * Deep copy.
   *
   * Clones this object, deep copies complex Data objects referenced by this object, and
   * update internal references to point to the deep copies.
   *
   * @throws CloneNotSupportedException if the object cannot be deep copied.
   */
  public abstract SpecificList copy() throws CloneNotSupportedException;

  protected ArrayList<Object> copyList(List<Object> source) throws CloneNotSupportedException
  {
    if (source == null)
    {
      return null;
    }

    ArrayList<Object> copy = new ArrayList<>();
    for (Object element : source)
    {
      copy.add(copy(element));
    }

    return copy;
  }

  protected final Object copy(Object value) throws CloneNotSupportedException
  {
    if (value == null || !Data.isComplex(value))
    {
      return value;
    }

    return ((DataComplex) value).copy();
  }
}
