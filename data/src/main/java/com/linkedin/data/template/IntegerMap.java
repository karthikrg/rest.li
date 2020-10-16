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
import com.linkedin.data.DataMap;
import com.linkedin.data.collections.CheckedUtil;
import com.linkedin.data.collections.SpecificDataComplexProvider;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.util.ArgumentUtil;
import java.io.IOException;
import java.util.Map;


/**
 * {@link DataTemplate} for a map with integer values.
 */
public final class IntegerMap extends DirectMapTemplate<Integer>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new IntegerMapSpecificDataComplexProvider();
  private static final MapDataSchema SCHEMA = (MapDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"map\", \"values\" : \"int\" }");

  public IntegerMap()
  {
    this(new DataMap(new IntegerSpecificValueMap()));
  }

  public IntegerMap(int initialCapacity)
  {
    this(new DataMap(new IntegerSpecificValueMap(initialCapacity)));
  }

  public IntegerMap(int initialCapacity, float loadFactor)
  {
    this(new DataMap(new IntegerSpecificValueMap(initialCapacity, loadFactor)));
  }

  public IntegerMap(Map<String, Integer> m)
  {
    this(capacityFromSize(m.size()));
    putAll(m);
  }

  public IntegerMap(DataMap map)
  {
    super(map, SCHEMA, Integer.class, Integer.class);
  }

  public void put(String key, int value)
  {
    CheckedUtil.putWithoutChecking(_map, key, value);
  }

  @Override
  public IntegerMap clone() throws CloneNotSupportedException
  {
    return (IntegerMap) super.clone();
  }

  @Override
  public IntegerMap copy() throws CloneNotSupportedException
  {
    return (IntegerMap) super.copy();
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
    return DataTemplateUtil.coerceIntOutput(object);
  }

  public static class IntegerSpecificValueMap extends SpecificValueMapTemplate<Integer>
  {
    public IntegerSpecificValueMap()
    {
      super(Integer.class);
    }

    public IntegerSpecificValueMap(int capacity)
    {
      super(capacity, Integer.class);
    }

    public IntegerSpecificValueMap(int capacity, float loadFactor)
    {
      super(capacity, loadFactor, Integer.class);
    }

    @Override
    protected void specificTraverse(Integer object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.integerValue(object);
    }
  }

  private static class IntegerMapSpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public Map<String, Object> getMap()
    {
      return new IntegerSpecificValueMap();
    }

    @Override
    public Map<String, Object> getMap(int capacity)
    {
      return new IntegerSpecificValueMap(capacity);
    }
  }
}