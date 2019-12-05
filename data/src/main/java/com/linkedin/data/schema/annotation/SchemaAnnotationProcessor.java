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

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.SchemaVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This SchemaAnnotationProcessor is for processing annotations in {@link DataSchema}.
 *
 * The processor is expected to take {@link SchemaAnnotationHandler} as arguments, use them with {@link SchemaVisitor} to
 * traverse the schema and call the {@link SchemaVisitor#callbackOnContext(DataSchemaRichContextTraverser.TraverserContext, DataSchemaTraverse.Order)}
 * on the {@link SchemaAnnotationHandler}
 *
 * If the schema annotation is annotated using syntax rule that uses pathSpec as path for overriding fields,
 * Then the {@link PathSpecBasedSchemaAnnotationVisitor} can be used to parse such rules
 * And in this case,  what users would need to implement is a {@link SchemaAnnotationHandler} that extends from {@link PathSpecBasedSchemaAnnotationVisitor}.
 *
 * also see {@link PathSpecBasedSchemaAnnotationVisitor} for overriding annotation using pathspec
 * also see {@link SchemaAnnotationHandler} for what to implement as resolution logic
 *
 */
public class SchemaAnnotationProcessor
{
  private static final Logger LOG = LoggerFactory.getLogger(SchemaAnnotationProcessor.class);

  /**
   * This function creates  {@link DataSchemaRichContextTraverser} and use it to wrap {@link SchemaVisitor} to visit the {@link DataSchema}
   *
   * Note {@link SchemaAnnotationHandler}'s #resolve() and #validate() function are supposed to be called by {@link SchemaVisitor}
   *
   * For the given {@link DataSchema}, it will first invoke each {@link SchemaAnnotationHandler#resolve}
   * by using the {@link SchemaVisitor} returned by {@link SchemaAnnotationHandler#getVisitor()}
   *
   * then it uses {@link SchemaAnnotationValidationVisitor} to invoke each {@link SchemaAnnotationHandler#validate} to validate resolved schema annotation.
   *
   * It will abort in case of unexpected exceptions.
   * Otherwise will aggregate error messages after all handlers' processing, to the final {@link SchemaAnnotationProcessResult}
   *
   * @param handlers the handlers that can resolve the annotation on the dataSchema and validate them
   * @param dataSchema the dataSchema to be processed
   * @param options additional options to help schema annotation processing
   * @return result after process
   */
  public static SchemaAnnotationProcessResult process(List<SchemaAnnotationHandler> handlers,
                                                      DataSchema dataSchema, AnnotationProcessOption options)
  {

    SchemaAnnotationProcessResult processResult = new SchemaAnnotationProcessResult();
    processResult.setResultSchema(dataSchema);
    StringBuilder errorMsgBuilder = new StringBuilder();


    // resolve
    boolean hasResolveError = false;
    for (SchemaAnnotationHandler schemaAnnotationHandler: handlers)
    {
      DataSchema schemaToProcess = processResult.getResultSchema();
      LOG.debug("DEBUG:  starting validating {} handler", schemaAnnotationHandler.getAnnotationNamespace());
      SchemaVisitor visitor = schemaAnnotationHandler.getVisitor();
      DataSchemaRichContextTraverser traverser = new DataSchemaRichContextTraverser(visitor);
      try
      {
        traverser.traverse(schemaToProcess);
      }
      catch (Exception e)
      {
        throw new IllegalStateException(String.format("Annotation processing failed when resolving annotations in the schema using the handler for " +
                                                      "annotation namespace \"%s\"",
                                                      schemaAnnotationHandler.getAnnotationNamespace()), e);
      }
      DataSchemaRichContextTraverser.VisitorTraversalResult handlerTraverseResult = visitor.getVisitorTraversalResult();
      if (!handlerTraverseResult.isTraversalSuccessful())
      {
        hasResolveError = true;
        String errorMsgs = handlerTraverseResult.formatToErrorMessage();

        errorMsgBuilder.append(String.format("Annotation processing encountered errors during resolution in \"%s\" handler. \n",
                                             schemaAnnotationHandler.getAnnotationNamespace()));
        errorMsgBuilder.append(errorMsgs);
      } else
      {
        DataSchema visitorConstructedSchema = handlerTraverseResult.getConstructedSchema();
        if (visitorConstructedSchema != null)
        {
          // will store the constructed dataSchema from the visitor in the processResult.
          processResult.setResultSchema(visitorConstructedSchema);
        }
      }
    }
    processResult.setResolutionSuccess(!hasResolveError);
    // early terminate if resolution failed
    if (!processResult.isResolutionSuccess())
    {
      errorMsgBuilder.append("Annotation processing failed when processing resolution by at least one of the handlers");
      String errorMsg = errorMsgBuilder.toString();
      LOG.error(errorMsg);
      processResult.setErrorMsgs(errorMsg);
      return processResult;
    }

    // validate
    boolean hasValidationError = false;
    for (SchemaAnnotationHandler schemaAnnotationHandler: handlers)
    {

      LOG.debug("DEBUG:  starting validating using {} handler", schemaAnnotationHandler.getAnnotationNamespace());
      SchemaAnnotationValidationVisitor validationVisitor = new SchemaAnnotationValidationVisitor(schemaAnnotationHandler);
      DataSchemaRichContextTraverser traverserBase = new DataSchemaRichContextTraverser(validationVisitor);
      try {
        traverserBase.traverse(processResult.getResultSchema());
      }
      catch (Exception e)
      {
        throw new IllegalStateException(String.format("Annotation processing failed when validating the schema using handler %s",
                                                      schemaAnnotationHandler.getAnnotationNamespace()), e);
      }
      DataSchemaRichContextTraverser.VisitorTraversalResult handlerTraverseResult = validationVisitor.getVisitorTraversalResult();
      if (!handlerTraverseResult.isTraversalSuccessful())
      {
        hasValidationError = true;

        String errorMsgs = handlerTraverseResult.formatToErrorMessage();

        errorMsgBuilder.append(String.format("Annotation processing encountered errors during validation in \"%s\" handler. \n",
                                             schemaAnnotationHandler.getAnnotationNamespace()));
        errorMsgBuilder.append(errorMsgs);
      }
    }
    processResult.setValidationSuccess(!hasValidationError);
    processResult.setErrorMsgs(errorMsgBuilder.toString());
    return processResult;
  }


  /**
   * Util function to get the resolvedProperties of the field specified by the PathSpec from the dataSchema.
   * If the path specified is invalid for the given dataSchema, will throw {@link IllegalArgumentException}
   *
   * @param pathSpec the pathSpec to search
   * @param dataSchema the dataSchema to start searching from
   * @return the resolvedProperties map
   */
  public static Map<String, Object> getResolvedPropertiesByPath(String pathSpec, DataSchema dataSchema)
  {
    List<String> pathComponents = new ArrayList<>(Arrays.asList(pathSpec.split(Character.toString(PathSpec.SEPARATOR))));
    if (!PathSpec.validatePathSpecString(pathSpec) || pathComponents.size() == 0)
    {
      throw new IllegalArgumentException(String.format("Invalid PathSpec %s", pathSpec));
    }
    pathComponents.remove("");
    DataSchema dataSchemaToPath = findDataSchemaByPath(dataSchema, pathComponents, pathSpec);
    return dataSchemaToPath.getResolvedProperties();
  }

  private static DataSchema findDataSchemaByPath(DataSchema dataSchema, List<String> paths, String pathSpec)
  {

    if (paths.size() == 0 )
    {
      if ((dataSchema.getType() == DataSchema.Type.TYPEREF))
      {
        dataSchema = dataSchema.getDereferencedDataSchema();
      }
      return dataSchema;
    }

    for (String pathSegment: paths)
    {
      if (dataSchema != null)
      {
        while ((dataSchema.getType() == DataSchema.Type.TYPEREF))
        {
          dataSchema = ((TyperefDataSchema) dataSchema).getRef();
        }
        switch (dataSchema.getType())
        {
          case RECORD:
            RecordDataSchema recordDataSchema = (RecordDataSchema) dataSchema;
            RecordDataSchema.Field field = recordDataSchema.getField(pathSegment);
            if (field == null)
            {
              break;
            } else
            {
              dataSchema = field.getType();
              continue;
            }
          case UNION:
            UnionDataSchema unionDataSchema = (UnionDataSchema) dataSchema;
            DataSchema unionSchema = unionDataSchema.getTypeByMemberKey(pathSegment);
            if (unionSchema == null)
            {
              break;
            } else
            {
              dataSchema = unionSchema;
              continue;
            }
          case MAP:
            if (pathSegment.equals(PathSpec.WILDCARD))
            {
              dataSchema = ((MapDataSchema) dataSchema).getValues();
              continue;
            } else if (pathSegment.equals((DataSchemaConstants.MAP_KEY_REF)))
            {
              dataSchema = ((MapDataSchema) dataSchema).getKey();
              continue;
            } else
            {
              break;
            }
          case ARRAY:
            if (pathSegment.equals(PathSpec.WILDCARD))
            {
              dataSchema = ((ArrayDataSchema) dataSchema).getItems();
              continue;
            } else
            {
              break;
            }
          default:
            //illegal state
            break;
        }
      }
      // Exception will be thrown if the paths are too long, or the path segment could not match to a data schema
      String errorMsg = String.format("Could not find path segment {%s} in PathSpec {%s}", pathSegment, pathSpec);
      throw new IllegalArgumentException(errorMsg);
    }

    if ((dataSchema.getType() == DataSchema.Type.TYPEREF))
    {
      dataSchema = dataSchema.getDereferencedDataSchema();
    }

    return dataSchema;
  }


  /**
   * Process result returned by {@link #process(List, DataSchema, AnnotationProcessOption)}
   */
  public static class SchemaAnnotationProcessResult
  {
    SchemaAnnotationProcessResult()
    {
    }

    public DataSchema getResultSchema()
    {
      return _resultSchema;
    }

    public void setResultSchema(DataSchema resultSchema)
    {
      _resultSchema = resultSchema;
    }

    public boolean hasError()
    {
      return !(_resolutionSuccess && _validationSuccess);
    }

    public boolean isResolutionSuccess()
    {
      return _resolutionSuccess;
    }

    void setResolutionSuccess(boolean resolutionSuccess)
    {
      _resolutionSuccess = resolutionSuccess;
    }

    public boolean isValidationSuccess()
    {
      return _validationSuccess;
    }

    public void setValidationSuccess(boolean validationSuccess)
    {
      _validationSuccess = validationSuccess;
    }

    public String getErrorMsgs()
    {
      return errorMsgs;
    }

    public void setErrorMsgs(String errorMsgs)
    {
      this.errorMsgs = errorMsgs;
    }

    DataSchema _resultSchema;
    boolean _resolutionSuccess = false;
    boolean _validationSuccess = false;
    String errorMsgs;
  }

  /***
   * Additional options to pass to help processing schema annotations
   */
  public static class AnnotationProcessOption
  {

  }
}
