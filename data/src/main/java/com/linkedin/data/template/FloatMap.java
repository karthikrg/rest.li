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
 * {@link DataTemplate} for a map with float values.
 */
public final class FloatMap extends DirectMapTemplate<Float>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new FloatMapSpecificDataComplexProvider();
  private static final MapDataSchema SCHEMA = (MapDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"map\", \"values\" : \"float\" }");

  public FloatMap()
  {
    this(new DataMap(new FloatSpecificValueMap()));
  }

  public FloatMap(int initialCapacity)
  {
    this(new DataMap(new FloatSpecificValueMap(initialCapacity)));
  }

  public FloatMap(int initialCapacity, float loadFactor)
  {
    this(new DataMap(new FloatSpecificValueMap(initialCapacity, loadFactor)));
  }

  public FloatMap(Map<String, Float> m)
  {
    this(capacityFromSize(m.size()));
    putAll(m);
  }

  public FloatMap(DataMap map)
  {
    super(map, SCHEMA, Float.class, Float.class);
  }

  @Override
  public FloatMap clone() throws CloneNotSupportedException
  {
    return (FloatMap) super.clone();
  }

  public void put(String key, float value)
  {
    CheckedUtil.putWithoutChecking(_map, key, value);
  }

  @Override
  public FloatMap copy() throws CloneNotSupportedException
  {
    return (FloatMap) super.copy();
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
    return DataTemplateUtil.coerceFloatOutput(object);
  }

  public static class FloatSpecificValueMap extends SpecificValueMapTemplate<Float>
  {
    public FloatSpecificValueMap()
    {
      super(Float.class);
    }

    public FloatSpecificValueMap(int capacity)
    {
      super(capacity, Float.class);
    }

    public FloatSpecificValueMap(int capacity, float loadFactor)
    {
      super(capacity, loadFactor, Float.class);
    }

    @Override
    protected void specificTraverse(Float object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.floatValue(object);
    }
  }

  private static class FloatMapSpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public Map<String, Object> getMap()
    {
      return new FloatSpecificValueMap();
    }

    @Override
    public Map<String, Object> getMap(int capacity)
    {
      return new FloatSpecificValueMap(capacity);
    }
  }
}