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
 * {@link DataTemplate} for a double array.
 */
public final class DoubleArray extends DirectArrayTemplate<Double>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new DoubleArraySpecificDataComplexProvider();
  private static final ArrayDataSchema SCHEMA = (ArrayDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"array\", \"items\" : \"double\" }");

  public DoubleArray()
  {
    this(new DataList(new DoubleSpecificElementArray()));
  }

  public DoubleArray(int initialCapacity)
  {
    this(new DataList(new DoubleSpecificElementArray(initialCapacity)));
  }

  public DoubleArray(Collection<Double> c)
  {
    this(new DataList(new DoubleSpecificElementArray(c.size())));
    addAll(c);
  }

  public DoubleArray(DataList list)
  {
    super(list, SCHEMA, Double.class, Double.class);
  }

  public DoubleArray(Double first, Double... rest)
  {
    this(new DataList(new DoubleSpecificElementArray(rest.length + 1)));
    add(first);
    addAll(Arrays.asList(rest));
  }

  public boolean add(double element) throws ClassCastException
  {
    return CheckedUtil.addWithoutChecking(_list, element);
  }

  public void add(int index, double element) throws ClassCastException
  {
    CheckedUtil.addWithoutChecking(_list, index, element);
  }

  @Override
  public DoubleArray clone() throws CloneNotSupportedException
  {
    return (DoubleArray) super.clone();
  }

  @Override
  public DoubleArray copy() throws CloneNotSupportedException
  {
    return (DoubleArray) super.copy();
  }

  @Override
  protected Object coerceInput(Double object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return DataTemplateUtil.coerceDoubleInput(object);
  }

  @Override
  protected Double coerceOutput(Object object) throws TemplateOutputCastException
  {
    assert(object != null);
    return DataTemplateUtil.coerceDoubleOutput(object);
  }

  public static class DoubleSpecificElementArray extends SpecificElementArrayTemplate<Double>
  {
    public DoubleSpecificElementArray()
    {
      super(Double.class);
    }

    public DoubleSpecificElementArray(int capacity)
    {
      super(capacity, Double.class);
    }

    @Override
    protected void specificTraverse(Double object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.doubleValue(object);
    }
  }

  private static class DoubleArraySpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public List<Object> getList()
    {
      return new DoubleSpecificElementArray();
    }

    @Override
    public List<Object> getList(int capacity)
    {
      return new DoubleSpecificElementArray(capacity);
    }
  }
}
