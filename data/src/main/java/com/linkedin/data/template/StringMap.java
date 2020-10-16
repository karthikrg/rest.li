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
import com.linkedin.data.collections.SpecificDataComplexProvider;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.util.ArgumentUtil;
import java.io.IOException;
import java.util.Map;


/**
 * {@link DataTemplate} for a map with string values.
 */
public final class StringMap extends DirectMapTemplate<String>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new StringMapSpecificDataComplexProvider();
  private static final MapDataSchema SCHEMA = (MapDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"map\", \"values\" : \"string\" }");

  public StringMap()
  {
    this(new DataMap(new StringSpecificValueMap()));
  }

  public StringMap(int initialCapacity)
  {
    this(new DataMap(new StringSpecificValueMap(initialCapacity)));
  }

  public StringMap(int initialCapacity, float loadFactor)
  {
    this(new DataMap(new StringSpecificValueMap(initialCapacity, loadFactor)));
  }

  public StringMap(Map<String, String> m)
  {
    this(capacityFromSize(m.size()));
    putAll(m);
  }

  public StringMap(DataMap map)
  {
    super(map, SCHEMA, String.class, String.class);
  }

  @Override
  public StringMap clone() throws CloneNotSupportedException
  {
    return (StringMap) super.clone();
  }

  @Override
  public StringMap copy() throws CloneNotSupportedException
  {
    return (StringMap) super.copy();
  }

  @Override
  protected Object coerceInput(String object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return object;
  }

  @Override
  protected String coerceOutput(Object object) throws TemplateOutputCastException
  {
    return DataTemplateUtil.coerceStringOutput(object);
  }

  public static class StringSpecificValueMap extends SpecificValueMapTemplate<String>
  {
    public StringSpecificValueMap()
    {
      super(String.class);
    }

    public StringSpecificValueMap(int capacity)
    {
      super(capacity, String.class);
    }

    public StringSpecificValueMap(int capacity, float loadFactor)
    {
      super(capacity, loadFactor, String.class);
    }

    @Override
    protected void specificTraverse(String object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.stringValue(object);
    }
  }

  private static class StringMapSpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public Map<String, Object> getMap()
    {
      return new StringSpecificValueMap();
    }

    @Override
    public Map<String, Object> getMap(int capacity)
    {
      return new StringSpecificValueMap(capacity);
    }
  }
}