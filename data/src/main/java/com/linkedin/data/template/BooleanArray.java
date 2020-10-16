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
 * {@link DataTemplate} for a boolean array.
 */
public final class BooleanArray extends DirectArrayTemplate<Boolean>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new BooleanArraySpecificDataComplexProvider();
  private static final ArrayDataSchema SCHEMA = (ArrayDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"array\", \"items\" : \"boolean\" }");

  public BooleanArray()
  {
    this(new DataList(new BooleanSpecificElementArray()));
  }

  public BooleanArray(int initialCapacity)
  {
    this(new DataList(new BooleanSpecificElementArray(initialCapacity)));
  }

  public BooleanArray(Collection<Boolean> c)
  {
    this(new DataList(new BooleanSpecificElementArray(c.size())));
    addAll(c);
  }

  public BooleanArray(DataList list)
  {
    super(list, SCHEMA, Boolean.class, Boolean.class);
  }

  public BooleanArray(Boolean first, Boolean... rest)
  {
    this(new DataList(new BooleanSpecificElementArray(rest.length + 1)));
    add(first);
    addAll(Arrays.asList(rest));
  }

  public boolean add(boolean element) throws ClassCastException
  {
    return CheckedUtil.addWithoutChecking(_list, element);
  }

  public void add(int index, boolean element) throws ClassCastException
  {
    CheckedUtil.addWithoutChecking(_list, index, element);
  }

  @Override
  public BooleanArray clone() throws CloneNotSupportedException
  {
    return (BooleanArray) super.clone();
  }

  @Override
  public BooleanArray copy() throws CloneNotSupportedException
  {
    return (BooleanArray) super.copy();
  }

  @Override
  protected Object coerceInput(Boolean object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return object;
  }

  @Override
  protected Boolean coerceOutput(Object object) throws TemplateOutputCastException
  {
    assert(object != null);
    return DataTemplateUtil.coerceBooleanOutput(object);
  }

  public static class BooleanSpecificElementArray extends SpecificElementArrayTemplate<Boolean>
  {
    public BooleanSpecificElementArray()
    {
      super(Boolean.class);
    }

    public BooleanSpecificElementArray(int capacity)
    {
      super(capacity, Boolean.class);
    }

    @Override
    protected void specificTraverse(Boolean object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.booleanValue(object);
    }
  }

  private static class BooleanArraySpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public List<Object> getList()
    {
      return new BooleanSpecificElementArray();
    }

    @Override
    public List<Object> getList(int capacity)
    {
      return new BooleanSpecificElementArray(capacity);
    }
  }
}
