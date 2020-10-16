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
 * {@link DataTemplate} for a map with boolean values.
 */
public final class BooleanMap extends DirectMapTemplate<Boolean>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new BooleanMapSpecificDataComplexProvider();
  private static final MapDataSchema SCHEMA = (MapDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"map\", \"values\" : \"boolean\" }");

  public BooleanMap()
  {
    this(new DataMap(new BooleanSpecificValueMap()));
  }

  public BooleanMap(int initialCapacity)
  {
    this(new DataMap(new BooleanSpecificValueMap(initialCapacity)));
  }

  public BooleanMap(int initialCapacity, float loadFactor)
  {
    this(new DataMap(new BooleanSpecificValueMap(initialCapacity, loadFactor)));
  }

  public BooleanMap(Map<String, Boolean> m)
  {
    this(capacityFromSize(m.size()));
    putAll(m);
  }

  public BooleanMap(DataMap map)
  {
    super(map, SCHEMA, Boolean.class, Boolean.class);
  }

  public void put(String key, boolean value)
  {
    CheckedUtil.putWithoutChecking(_map, key, value);
  }

  @Override
  public BooleanMap clone() throws CloneNotSupportedException
  {
    return (BooleanMap) super.clone();
  }

  @Override
  public BooleanMap copy() throws CloneNotSupportedException
  {
    return (BooleanMap) super.copy();
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
    return DataTemplateUtil.coerceBooleanOutput(object);
  }

  public static class BooleanSpecificValueMap extends SpecificValueMapTemplate<Boolean>
  {
    public BooleanSpecificValueMap()
    {
      super(Boolean.class);
    }

    public BooleanSpecificValueMap(int capacity)
    {
      super(capacity, Boolean.class);
    }

    public BooleanSpecificValueMap(int capacity, float loadFactor)
    {
      super(capacity, loadFactor, Boolean.class);
    }

    @Override
    protected void specificTraverse(Boolean object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.booleanValue(object);
    }
  }

  private static class BooleanMapSpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public Map<String, Object> getMap()
    {
      return new BooleanSpecificValueMap();
    }

    @Override
    public Map<String, Object> getMap(int capacity)
    {
      return new BooleanSpecificValueMap(capacity);
    }
  }
}