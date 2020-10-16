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
 * {@link DataTemplate} for a float array.
 */
public final class FloatArray extends DirectArrayTemplate<Float>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new FloatArraySpecificDataComplexProvider();
  private static final ArrayDataSchema SCHEMA = (ArrayDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"array\", \"items\" : \"float\" }");

  public FloatArray()
  {
    this(new DataList(new FloatSpecificElementArray()));
  }

  public FloatArray(int initialCapacity)
  {
    this(new DataList(new FloatSpecificElementArray(initialCapacity)));
  }

  public FloatArray(Collection<Float> c)
  {
    this(new DataList(new FloatSpecificElementArray(c.size())));
    addAll(c);
  }

  public FloatArray(DataList list)
  {
    super(list, SCHEMA, Float.class, Float.class);
  }

  public FloatArray(Float first, Float... rest)
  {
    this(new DataList(new FloatSpecificElementArray(rest.length + 1)));
    add(first);
    addAll(Arrays.asList(rest));
  }

  public boolean add(float element) throws ClassCastException
  {
    return CheckedUtil.addWithoutChecking(_list, element);
  }

  public void add(int index, float element) throws ClassCastException
  {
    CheckedUtil.addWithoutChecking(_list, index, element);
  }

  @Override
  public FloatArray clone() throws CloneNotSupportedException
  {
    return (FloatArray) super.clone();
  }

  @Override
  public FloatArray copy() throws CloneNotSupportedException
  {
    return (FloatArray) super.copy();
  }

  @Override
  protected Object coerceInput(Float object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return DataTemplateUtil.coerceFloatInput(object);
  }

  @Override
  protected Float coerceOutput(Object object) throws TemplateOutputCastException
  {
    assert(object != null);
    return DataTemplateUtil.coerceFloatOutput(object);
  }

  public static class FloatSpecificElementArray extends SpecificElementArrayTemplate<Float>
  {
    public FloatSpecificElementArray()
    {
      super(Float.class);
    }

    public FloatSpecificElementArray(int capacity)
    {
      super(capacity, Float.class);
    }

    @Override
    protected void specificTraverse(Float object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.floatValue(object);
    }
  }

  private static class FloatArraySpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public List<Object> getList()
    {
      return new FloatSpecificElementArray();
    }

    @Override
    public List<Object> getList(int capacity)
    {
      return new FloatSpecificElementArray(capacity);
    }
  }
}
