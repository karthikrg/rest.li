/*
   Copyright (c) 2019 LinkedIn Corp.

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
package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * This visitor will iterate over all leaf data schema which could contain stores resolvedProperties after data schemas
 * are processed.
 *
 * The resolvedProperties will be stored in a map. For example,
 * <pre>{@code
 * record Test {
 *    @customAnnotation = {
 *      "/f1/f2": "sth"
 *    }
 *    f0: record A {
 *      f1: A
 *      @AnotherAnnotation = "NONE"
 *      f2: string
 *    }
 *  }
 * }
 * </pre>
 *
 * One can expect following in the stored map
 * <pre>
 * {
 *   "/f0/f1/f1/f2": {
 *     "AnotherAnnotation" : "NONE"
 *   },
 *   "/f0/f1/f2": {
 *     "AnotherAnnotation" : "NONE"
 *     "customAnnotation" : "sth"
 *   }
 *   "f0/f2" : {
 *     "AnotherAnnotation" : "NONE"
 *   }
 * }
 * </pre>
 *
 * a leaf DataSchema is a schema that doesn't have other types of DataSchema linked from it.
 * Below types are leaf DataSchemas
 * {@link com.linkedin.data.schema.PrimitiveDataSchema} ,
 * {@link com.linkedin.data.schema.EnumDataSchema} ,
 * {@link com.linkedin.data.schema.FixedDataSchema}
 *
 * Other dataSchema types, for example {@link com.linkedin.data.schema.TyperefDataSchema} could link to another DataSchema
 * so it is not a leaf DataSchema *
 */
public class ResolvedPropertiesReaderVisitor implements DataSchemaRichContextTraverser.SchemaVisitor
{
  private Map<String, Map<String, Object>> _primitiveFieldsPathSpecToResolvedPropertiesCache = new HashMap<>();

  @Override
  public void callbackOnContext(DataSchemaRichContextTraverser.TraverserContext context, DataSchemaTraverse.Order order)
  {
    if (order == DataSchemaTraverse.Order.POST_ORDER)
    {
      return;
    }

    DataSchema currentSchema = context.getCurrentSchema();
    if (PathSpecBasedSchemaAnnotationVisitor.couldStoreResolvedPropertiesInSchema(currentSchema))
    {
      Map<String, Object> resolvedProperties = currentSchema.getResolvedProperties();
      _primitiveFieldsPathSpecToResolvedPropertiesCache.put(
          new PathSpec(context.getSchemaPathSpec().toArray(new String[0])).toString(), resolvedProperties);

      String mapStringified = resolvedProperties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(
          Collectors.joining("&"));
      System.out.println(String.format("/%s ::: %s", String.join("/", context.getSchemaPathSpec()), mapStringified));
    }
  }

  @Override
  public DataSchemaRichContextTraverser.VisitorContext getInitialVisitorContext()
  {
    return new DataSchemaRichContextTraverser.VisitorContext(){};
  }

  @Override
  public DataSchemaRichContextTraverser.VisitorTraversalResult getVisitorTraversalResult()
  {
    return null;
  }

  public Map<String, Map<String, Object>> getPrimitiveFieldsPathSpecToResolvedPropertiesCache()
  {
    return _primitiveFieldsPathSpecToResolvedPropertiesCache;
  }

  public void setPrimitiveFieldsPathSpecToResolvedPropertiesCache(
      Map<String, Map<String, Object>> primitiveFieldsPathSpecToResolvedPropertiesCache)
  {
    _primitiveFieldsPathSpecToResolvedPropertiesCache = primitiveFieldsPathSpecToResolvedPropertiesCache;
  }

}
