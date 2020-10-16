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

import com.linkedin.data.ByteString;
import com.linkedin.data.Data;
import com.linkedin.data.DataMap;
import com.linkedin.data.collections.SpecificDataComplexProvider;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.util.ArgumentUtil;
import java.io.IOException;
import java.util.Map;


/**
 * {@link DataTemplate} for a map with bytes values.
 */
public final class BytesMap extends DirectMapTemplate<ByteString>
{
  public static final SpecificDataComplexProvider SPECIFIC_DATA_COMPLEX_PROVIDER = new BytesMapSpecificDataComplexProvider();
  private static final MapDataSchema SCHEMA = (MapDataSchema) DataTemplateUtil.parseSchema("{ \"type\" : \"map\", \"values\" : \"bytes\" }");

  public BytesMap()
  {
    this(new DataMap(new ByteStringSpecificValueMap()));
  }

  public BytesMap(int initialCapacity)
  {
    this(new DataMap(new ByteStringSpecificValueMap(initialCapacity)));
  }

  public BytesMap(int initialCapacity, float loadFactor)
  {
    this(new DataMap(new ByteStringSpecificValueMap(initialCapacity, loadFactor)));
  }

  public BytesMap(Map<String, ByteString> m)
  {
    this(capacityFromSize(m.size()));
    putAll(m);
  }

  public BytesMap(DataMap map)
  {
    super(map, SCHEMA, ByteString.class, ByteString.class);
  }

  @Override
  public BytesMap clone() throws CloneNotSupportedException
  {
    return (BytesMap) super.clone();
  }

  @Override
  public BytesMap copy() throws CloneNotSupportedException
  {
    return (BytesMap) super.copy();
  }

  @Override
  protected Object coerceInput(ByteString object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    return object;
  }

  @Override
  protected ByteString coerceOutput(Object object) throws TemplateOutputCastException
  {
    return DataTemplateUtil.coerceBytesOutput(object);
  }

  public static class ByteStringSpecificValueMap extends SpecificValueMapTemplate<ByteString>
  {
    public ByteStringSpecificValueMap()
    {
      super(ByteString.class);
    }

    public ByteStringSpecificValueMap(int capacity)
    {
      super(capacity, ByteString.class);
    }

    public ByteStringSpecificValueMap(int capacity, float loadFactor)
    {
      super(capacity, loadFactor, ByteString.class);
    }

    @Override
    protected void specificTraverse(ByteString object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      callback.byteStringValue(object);
    }
  }

  private static class BytesMapSpecificDataComplexProvider implements SpecificDataComplexProvider
  {
    @Override
    public Map<String, Object> getMap()
    {
      return new ByteStringSpecificValueMap();
    }

    @Override
    public Map<String, Object> getMap(int capacity)
    {
      return new ByteStringSpecificValueMap(capacity);
    }
  }
}