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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;


/**
 * This is a simple implementation of {@link SchemaAnnotationHandler}
 * It implements a simple resolution function, which replace with latest override, but not does no validation.
 *
 * User can choose to use this a default handler and only need to override and implement the "validate" method
 *
 */
public class PegasusSchemaAnnotationHandlerImpl implements SchemaAnnotationHandler
{
  private final String _annotationNameSpace;

  public PegasusSchemaAnnotationHandlerImpl(String annotationNameSpace)
  {
    _annotationNameSpace = annotationNameSpace;
  }

  @SuppressWarnings("serial")
  @Override
  public ResolutionResult resolve(List<Pair<String, Object>> propertiesOverrides, ResolutionMetaData resolutionMetaData)
  {
    ResolutionResult result = new ResolutionResult();
    if (propertiesOverrides.get(0) == null)
    {
      result.setError(true);
    } else
    {
      result.setResolvedResult(new HashMap<String, Object>()
      {{
        // simple implementation which takes the the first element from the overrides chain and put it to the map
        put(getAnnotationNamespace(), propertiesOverrides.get(0).getRight());
      }});
    }

    return result;
  }

  @Override
  public String getAnnotationNamespace()
  {
    return _annotationNameSpace;
  }

  @Override
  public AnnotationValidationResult validate(List<String> paths,
                                             Map<String, Object> resolvedProperties,
                                             DataSchema dataSchema,
                                             ValidationMetaData options)
  {
    return new AnnotationValidationResult();
  }

  @Override
  public DataSchemaRichContextTraverser.SchemaVisitor getVisitor()
  {
    return new PathSpecBasedSchemaAnnotationVisitor(this);
  }

}
