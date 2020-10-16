/*
   Copyright (c) 2012 LinkedIn Corp.

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

package com.linkedin.data.template;

import com.linkedin.data.Data;
import com.linkedin.data.DataList;
import com.linkedin.data.collections.CheckedUtil;
import com.linkedin.data.collections.SpecificDataComplexProvider;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.util.ArgumentUtil;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * {@link DataTemplate} for an integer array.
 */
public final class IntegerArray extends DirectArrayTemplate<Integer>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new IntegerArraySpecificDataComplexProvider();
  private static final ArrayDataSchema SCHEMA = (ArrayDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"array\", \"items\" : \"int\" }");

  public IntegerArray()
  {
    this(new DataList(new IntegerSpecificElementArray()));
  }

  public IntegerArray(int initialCapacity)
  {
    this(new DataList(new IntegerSpecificElementArray(initialCapacity)));
  }

  public IntegerArray(Collection<Integer> c)
  {
    this(new DataList(new IntegerSpecificElementArray(c.size())));
    addAll(c);
  }

  public IntegerArray(DataList list)
  {
    super(list, SCHEMA, Integer.class, Integer.class);
  }

  public IntegerArray(Integer first, Integer... rest)
  {
    this(new DataList(new IntegerSpecificElementArray(rest.length + 1)));
    add(first);
    addAll(Arrays.asList(rest));
  }

  public boolean add(int element) throws ClassCastException
  {
    return CheckedUtil.addWithoutChecking(_list, element);
  }

  public void add(int index, int element) throws ClassCastException
  {
    CheckedUtil.addWithoutChecking(_list, index, element);
  }

  @Override
  public IntegerArray clone() throws CloneNotSupportedException
  {
    return (IntegerArray) super.clone();
  }

  @Override
  public IntegerArray copy() throws CloneNotSupportedException
  {
    return (IntegerArray) super.copy();
  }

  @Override
  protected Object coerceInput(Integer object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return DataTemplateUtil.coerceIntInput(object);
  }

  @Override
  protected Integer coerceOutput(Object object) throws TemplateOutputCastException
  {
    assert(object != null);
    return DataTemplateUtil.coerceIntOutput(object);
  }

  public static class IntegerSpecificElementArray extends SpecificElementArrayTemplate<Integer>
  {
    public IntegerSpecificElementArray()
    {
      super(Integer.class);
    }

    public IntegerSpecificElementArray(int capacity)
    {
      super(capacity, Integer.class);
    }

    @Override
    protected void specificTraverse(Integer object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.integerValue(object);
    }
  }

  private static class IntegerArraySpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public List<Object> getList()
    {
      return new IntegerSpecificElementArray();
    }

    @Override
    public List<Object> getList(int capacity)
    {
      return new IntegerSpecificElementArray(capacity);
    }
  }
}

