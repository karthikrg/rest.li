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
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.CurrentSchemaEntryMode;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.TraverserContext;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.VisitorContext;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.VisitorTraversalResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import static java.util.stream.Collectors.toList;


/**
 * SchemaAnnotationPathSpecResolutionParser is a {@link DataSchemaRichContextTraverser.SchemaVisitor} implementation
 * that check and parse PathSpec overrides during Schema traverse.
 *
 * For a schema that has fields that were annotated with certain annotation namespace, schema writers can override field's annotation
 * values using overriding. And overriding using following syntax could be interpreted and handled by this parser.
 *
 * Example pdl schema with overrides:
 *
 * <pre>{@code
 * @customAnnotation= {"/f1/f2" : "2rd layer" }
 * f: record rcd {
 *     @customAnnotation= {"/f2" : "1rd layer" }
 *     f1: record rcd2 {
 *         @customAnnotation = "OriginalValue"
 *         f2: string
 *     }
 * }
 * }
 * </pre>
 *
 * In this example, the annotation namespace being annotated here is "customAnnotation".
 * The string field f2's customAnnotation "OriginalValue" was overridden by its upper layer fields.
 * Both  `{"/f1/f2" : "2rd layer" }` and `{"/f2" : "1rd layer" }` are its overrides and
 * the overrides value is specified using PathSpec to point to the field to be overridden.
 *
 * The "originalValue" can be {@link com.linkedin.data.DataMap} or {@link com.linkedin.data.DataList} or primitive types
 * but the overrides needs to be a key-value pair, where the key is PathSpec string representation.
 *
 * also see {@link SchemaAnnotationHandler}
 *
 *
 */
public class PathSpecBasedSchemaAnnotationVisitor implements DataSchemaRichContextTraverser.SchemaVisitor
{
  private final SchemaAnnotationHandler _handler;
  private final VisitorTraversalResult _visitorTraversalResult = new VisitorTraversalResult();
  private final IdentityHashMap<DataSchema, DataSchema> _seenDataSchemaMapping = new IdentityHashMap<>();
  private DataSchema _schemaConstructed = null;
  /**
   * Use this data structure to store whether a record schema "A" has fields contains overrides to a record schema "B"
   * The key will only be record's full name, key will be a {@link Set} contains record's full name
   *
   * For example
   * <pre>{@code
   * record rcdA {
   *   @customAnnotation = {"/rcdAf3": ""} // rcdA overrides to rcdA
   *   rcdAf1: rcdA
   *   rcdAf2: record rcdB {
   *     @customAnnotation = {"/rcdAf3": ""} // rcdB overrides to rcdA
   *     rcdBf1: rcdA
   *     rcdBf2: string
   *   }
   *   rcdAf3: string
   *   @customAnnotation = {"/rcdBf2": ""} // rcdA overrides to rcdB
   *   rcdAf4: rcdB
   * }
   * }
   * </pre>
   *
   * The directedEdges should have entries
   * <pre>
   * {
   *   "rcdA": set(["rcdA", "rcdB"])
   *   "rcdB": set(["rcdA"])
   * }
   * </pre>
   *
   */
  private Map<String, Set<String>> _directedEdges = new HashMap<>();
  private HashSet<HashSet<String>> _cycleCache = new HashSet<>();

  /**
   * If the schema A has annotations overrides that resolves to schema B and its descendents.
   * We say we have an directed edge from A to B.
   *
   * Given a directed edge and a map storing all edges seen, this function detects whether adding new edge to the edge map would
   * produce any cycles.
   *
   * @param startSchemaName the schema name of the start of edge
   * @param endSchemaName the schema name fo the end of the dge
   * @return
   */
  private boolean detectCycle(String startSchemaName, String endSchemaName)
  {
    if (startSchemaName.equals(endSchemaName) || _cycleCache.contains(new HashSet<>(Arrays.asList(startSchemaName, endSchemaName))))
    {
      return true;
    }

    // There were no cycles before checking this edge(startSchemaName -> endSchemaName) ,
    // So the goal is see if can find path (endSchemaName -> startSchemaName)
    HashSet<String> visited = new HashSet<>();
    boolean wouldFormCycle = dfs(endSchemaName, startSchemaName, visited, _directedEdges);

    if (wouldFormCycle)
    {
      _cycleCache.add(new HashSet<>(Arrays.asList(startSchemaName, endSchemaName)));
    }
    return wouldFormCycle;
  }

  /**
   * DFS routine to check if the targetSchemaName is found
   * @param currentSchemaName the current schema name being searched
   * @param targetSchemaName  the target schema name
   * @param visited a hashSet holds visited schema names
   * @param edges a map tells whether schema A has annotation overrides that could resolve to schema B and its descendents (i.e edge A-B)
   * @return whether the targetSchemaName was found using recursive dfs search
   */
  private boolean dfs(String currentSchemaName, String targetSchemaName, HashSet<String> visited, Map<String, Set<String>> edges)
  {
    visited.add(currentSchemaName);

    if (currentSchemaName.equals(targetSchemaName))
    {
      return true;
    }
    Set<String> nextNodes = edges.computeIfAbsent(currentSchemaName, key -> new HashSet<>());
    for (String nextNode: nextNodes)
    {
      if (!visited.contains(nextNode))
      {
        if (dfs(nextNode, targetSchemaName, visited, edges))
        {
          return true;
        }
      }
    }
    return false;
  }


  public PathSpecBasedSchemaAnnotationVisitor(SchemaAnnotationHandler handler)
  {
    _handler = handler;
  }

  @Override
  public VisitorContext getInitialVisitorContext()
  {
    return new PathSpecTraverseVisitorContext();
  }

  @Override
  public VisitorTraversalResult getVisitorTraversalResult()
  {
    return _visitorTraversalResult;
  }

  @Override
  public DataSchema getConstructedSchema()
  {
    return _schemaConstructed;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order)
  {

    if (order == DataSchemaTraverse.Order.POST_ORDER)
    {
      // Use post order visit to validate override paths
      VisitorContext postVisitContext = context.getVisitorContext();
      List<PathStruct> pathStructs =
          ((PathSpecTraverseVisitorContext) postVisitContext).getLastLayerOverridesPathStructs();

      for (PathStruct pathStruct: pathStructs)
      {
        if (pathStruct.isOverride() &&
            (pathStruct.getOverridePathValidStatus() == PathStruct.OverridePathValidStatus.UNCHECKED))
        {
          // invalid override path reporting
          pathStruct.setOverridePathValidStatus(PathStruct.OverridePathValidStatus.INVALID);
          getVisitorTraversalResult().addMessage(pathStruct.getPathOfOrigin(),
                                                 "Overriding pathSpec defined %s does not point to a valid primitive field",
                                                 pathStruct.getOriginalPathString());
        }
      }
      return;
    }

    VisitorContext visitorContext = context.getVisitorContext();
    //Prepare visitorContext for next level recursion
    PathSpecTraverseVisitorContext newVisitorContext = new PathSpecTraverseVisitorContext();
    // {@link PathSpecBasedSchemaAnnotationVisitor} will build new skeleton schema when visiting {@link TraversalContext}
    // If there has been a skeleton schema already built for one data schema, it will reuse that cached one
    // also see {@link PathSpecTraverseVisitorContext}
    DataSchema newSkeletonSchemaOrReUse = null;
    DataSchema parentSchema = context.getParentSchema();
    DataSchema currentSchema = context.getCurrentSchema();
    List<PathStruct> currentOverrides = new ArrayList<>();

    if (parentSchema == null)
    {
      try
      {
        newSkeletonSchemaOrReUse = CopySchemaUtil.buildSkeletonSchema(currentSchema);
        _seenDataSchemaMapping.put(currentSchema, newSkeletonSchemaOrReUse);
      } catch (CloneNotSupportedException e)
      {
        throw new IllegalStateException(
            String.format("encounter unexpected CloneNotSupportedException at traverse path location %s",
                          Arrays.toString(context.getTraversePath().toArray())), e);
      }
      _schemaConstructed = newSkeletonSchemaOrReUse;
      // in top level schema, only run this function on EnumDataSchema and FixedDataSchema:
      // if found EnumDataSchema and FixedDataSchema, will directly use the properties in the schema and resolve to resolvedProperties
      if (newSkeletonSchemaOrReUse.getType() == DataSchema.Type.FIXED || newSkeletonSchemaOrReUse.getType() == DataSchema.Type.ENUM)
      {
        Object property = currentSchema.getProperties().get(getAnnotationNamespace());
        if (property != null)
        {
          newSkeletonSchemaOrReUse.getResolvedProperties().put(getAnnotationNamespace(), property);
        }
      }

    } else
    {
      RecordDataSchema.Field enclosingField = context.getEnclosingField();
      String pathSpecMatchingSegment;

      if (parentSchema.getType() != DataSchema.Type.TYPEREF)
      {
        // PathSpec shouldn't include TypeRef segment
        pathSpecMatchingSegment = context.getSchemaPathSpec().peekLast();
      } else
      {
        pathSpecMatchingSegment = ((TyperefDataSchema) parentSchema).getFullName();
      }

      currentOverrides = ((PathSpecTraverseVisitorContext) visitorContext).getLastLayerOverridesPathStructs()
                                                                          .stream()
                                                                          .filter(pathStruct ->
                                                                                      pathStruct.getRemainingPaths().size() > 0 &&
                                                                                      Objects.equals(pathStruct.getRemainingPaths().peekFirst(),
                                                                                          pathSpecMatchingSegment))
                                                                          .collect(toList());

      currentOverrides.forEach(pathStruct ->
                               {
                                 pathStruct.getMatchedPaths().add(pathSpecMatchingSegment);
                                 pathStruct.getRemainingPaths().pollFirst();
                               });

      assert(currentOverrides.stream()
                             .filter(PathStruct::isOverride)
                             .allMatch(pathStruct -> pathStruct.getOverridePathValidStatus() == PathStruct.OverridePathValidStatus.UNCHECKED));

      switch (parentSchema.getType())
      {
        case RECORD:
          ArrayDeque<String> fullTraversePath = new ArrayDeque<>(context.getTraversePath());
          // Need to exclude this currentSchema's path so that it is field's path
          fullTraversePath.pollLast();
          currentOverrides.addAll(generatePathStructFromField(enclosingField, fullTraversePath));

          break;
        case TYPEREF:
          currentOverrides.addAll(generatePathStructFromTypeRefSchema((TyperefDataSchema) parentSchema,
                                                                      context.getTraversePath()));
          break;
        default:
          break;
      }

      if (currentSchema.getType() == DataSchema.Type.TYPEREF)
      {
        // Add child TypeRef full schema name to all currentOverrides
        // So they can be recognized when visiting the child TypeRef
        String typeRefComponentForPathStruct = ((TyperefDataSchema) currentSchema).getFullName();
        currentOverrides.forEach(pathStruct -> pathStruct.getRemainingPaths().addFirst(typeRefComponentForPathStruct));
      }

      // Below is for cyclic referencing checking:
      // after merging the override paths from the RecordDataSchema's fields
      // need to check whether this will produce cyclic overriding
      // If the schema A has annotations overrides that resolves to schema B and its descendents
      // We say we have an directed edge from A to B
      // So cyclic referencing detection is to detect whether such edges already seen are forming any cycles
      // Note: Cyclic referencing in TypeRef is handled in the similar way, de-referenced Schema of parent schema is the currentSchema here
      if (currentSchema.getType() == DataSchema.Type.RECORD)
      {

        String childSchemaFullName = ((RecordDataSchema) currentSchema).getFullName();
        for (PathStruct pathStruct: currentOverrides)
        {
          String overrideStartSchemaName = pathStruct.getStartSchemaName();
          String overrideEndSchemaName = childSchemaFullName;
          if (detectCycle(overrideStartSchemaName, overrideEndSchemaName))
          {
            //If cycles found, report errors
            getVisitorTraversalResult().addMessage(context.getTraversePath(), "Found overrides that forms " +
                                                                                "a cyclic-referencing: Overrides entry in " +
                                                                                "traverser path \"%s\" with its pathSpec value \"%s\" is pointing to the field " +
                                                                                "with traverser path \"%s\" and schema name \"%s\", this is causing cyclic-referencing.",
                                                   new PathSpec(pathStruct.getPathOfOrigin().toArray(new String[0])).toString(),
                                                   pathStruct.getOriginalPathString(),
                                                   new PathSpec(context.getTraversePath().toArray(new String[0])).toString(),
                                                   childSchemaFullName);
            context.setShouldContinue(Boolean.FALSE);
            newVisitorContext.setLastLayerOverridesPathStructs(currentOverrides);
            context.setVisitorContext(newVisitorContext);
            return;
          } else
          {
            //If no cycles found1A, add to current edges seen
            _directedEdges.computeIfAbsent(overrideStartSchemaName, key -> new HashSet<>()).add(overrideEndSchemaName);
          }
        }
      }

      try
      {
        if (couldStoreResolvedPropertiesInSchema(currentSchema))
        {
          // for enum and fixed, the properties can be written to the schema itself, so need to add them to currentOverrides
          if (currentSchema.getProperties().get(getAnnotationNamespace()) != null)
          {
            currentOverrides.add(generatePathStructFromNamedSchema(currentSchema, context.getTraversePath()));
          }

          newSkeletonSchemaOrReUse = createSchemaAndAttachToParent(context, (currentOverrides.size() != 0));
          if (currentOverrides.size() != 0)
          {
            newSkeletonSchemaOrReUse.getResolvedProperties().putAll(resolvePathStruct(currentOverrides,
                                                                               context.getSchemaPathSpec())); // Actually use resolve rules here
          }

          //Do pathStruct validity checking
          for (PathStruct pathStruct : currentOverrides)
          {
            if (pathStruct.isOverride())
            {
              if (pathStruct.getRemainingPaths().size() == 0)
              {
                pathStruct.setOverridePathValidStatus(PathStruct.OverridePathValidStatus.VALID);
              } else
              {
                // invalid override path reporting
                pathStruct.setOverridePathValidStatus(PathStruct.OverridePathValidStatus.INVALID);
                getVisitorTraversalResult().addMessage(pathStruct.getPathOfOrigin(),
                                                       "Overriding pathSpec defined %s does not point to a valid primitive field: Path might be too long",
                                                       pathStruct.getOriginalPathString());
              }
            }
          }
        } else if (currentSchema.isComplex())
        {
          // Either all non-overrides to TypeRefDataSchema, or all overrides to other complex dataSchema
          assert(currentOverrides.stream().noneMatch(PathStruct::isOverride) ||
                 currentOverrides.stream().allMatch(PathStruct::isOverride));

          //Do pathStruct validity checking
          for (PathStruct pathStruct : currentOverrides)
          {
            if(pathStruct.isOverride() && (pathStruct.getRemainingPaths().size() == 0))
            {
              // invalid override path reporting
              pathStruct.setOverridePathValidStatus(PathStruct.OverridePathValidStatus.INVALID);
              getVisitorTraversalResult().addMessage(pathStruct.getPathOfOrigin(),
                                                     "Overriding pathSpec defined %s does not point to a valid primitive field: Path might be too short",
                                                     pathStruct.getOriginalPathString());
            }
          }

          if (currentOverrides.stream()
                              .anyMatch(pathStruct -> !pathStruct.isOverride() ||
                                                      (pathStruct.getOverridePathValidStatus() ==
                                                       PathStruct.OverridePathValidStatus.UNCHECKED)))
          {
            //If there are unresolved overrides that resolving to complex data schema
            //Need to tell the traverser to continue traversing
            newSkeletonSchemaOrReUse = createSchemaAndAttachToParent(context, true);
            context.setShouldContinue(Boolean.TRUE);
          }
          else
          {
            // Order matters: Need to check "seen" before creating new or reuse
            context.setShouldContinue(!_seenDataSchemaMapping.containsKey(currentSchema));
            newSkeletonSchemaOrReUse = createSchemaAndAttachToParent(context, false);
          }
        }
      } catch (CloneNotSupportedException e)
      {
        throw new IllegalStateException(String.format("encounter unexpected CloneNotSupportedException at traverse path location %s",
                                                      Arrays.toString(context.getTraversePath().toArray())), e);
      }
    }

    if (currentSchema.getType() == DataSchema.Type.RECORD && ((RecordDataSchema)currentSchema).getInclude().size() > 0)
    {
      //Process includes
      currentOverrides.addAll(generatePathStructFromInclude((RecordDataSchema) currentSchema, context.getTraversePath()));
    }

    context.setCurrentSchema(currentSchema);

    newVisitorContext.setLastLayerOverridesPathStructs(currentOverrides);
    newVisitorContext.setLastVisitedDataSchemaInSchemaConstructed(newSkeletonSchemaOrReUse);
    context.setVisitorContext(newVisitorContext);
  }


  private List<PathStruct> generatePathStructFromInclude(RecordDataSchema dataSchema,
                                                         ArrayDeque<String> pathOfOrigin)
  {

    //Include can only be overrides
    return constructOverridePathStructFromProperties(dataSchema.getProperties(),
                                                     PathStruct.TypeOfOrigin.OVERRIDE_RECORD_FOR_INCLUDE,
                                                     pathOfOrigin,
                                                     dataSchema,
                                                     dataSchema.getFullName());
  }


  private List<PathStruct> generatePathStructFromField(RecordDataSchema.Field field,
                                                       ArrayDeque<String> pathOfOrigin)
  {
    if ((couldStoreResolvedPropertiesInSchema(field.getType())))
    {
      if (field.getProperties().get(getAnnotationNamespace()) != null)
      {
        return constructNonOverridePathStructFromProperties(field.getName(),
                                                            field.getProperties().get(getAnnotationNamespace()),
                                                            PathStruct.TypeOfOrigin.NON_OVERRIDE_RECORD_FIELD,
                                                            pathOfOrigin,
                                                            field);
      }
      return new ArrayList<>();
    } else
    {
      // Overrides could only happen if the field's schema could not store resolvedProperties directly
      return constructOverridePathStructFromProperties(field.getProperties(),
                                                       PathStruct.TypeOfOrigin.OVERRIDE_RECORD_FIELD,
                                                       pathOfOrigin,
                                                       field,
                                                       field.getRecord().getFullName());
    }
  }

  private List<PathStruct> generatePathStructFromTypeRefSchema(TyperefDataSchema dataSchema,
                                                               ArrayDeque<String> pathOfOrigin)
  {
    List<PathStruct> typeRefPathStructs = new ArrayList<>();
    if (!couldStoreResolvedPropertiesInSchema(dataSchema.getDereferencedDataSchema()))
    {
      //Should treat as overriding
      List<PathStruct> pathStructToReturn = constructOverridePathStructFromProperties(dataSchema.getProperties(),
                                                                                      PathStruct.TypeOfOrigin.OVERRIDE_TYPE_REF_OVERRIDE,
                                                                                      pathOfOrigin,
                                                                                      dataSchema,
                                                                                      dataSchema.getFullName());
      typeRefPathStructs.addAll(pathStructToReturn);
      //Need to add this "virtual" matched path for TypeRef
      typeRefPathStructs.forEach(pathStruct -> pathStruct.getMatchedPaths().add(dataSchema.getFullName()));
    } else
    {
      if (dataSchema.getProperties().get(getAnnotationNamespace()) != null)
      {
        typeRefPathStructs.addAll(constructNonOverridePathStructFromProperties(dataSchema.getFullName(),
                                                            dataSchema.getProperties().get(getAnnotationNamespace()),
                                                            PathStruct.TypeOfOrigin.NON_OVERRIDE_TYPE_REF,
                                                            pathOfOrigin,dataSchema));
      }
    }

    return typeRefPathStructs;
  }

  private PathStruct generatePathStructFromNamedSchema(DataSchema dataSchema, ArrayDeque<String> pathOfOrigin)
  {
    PathStruct.TypeOfOrigin typeOfOrigin;
    switch(dataSchema.getType())
    {
      case FIXED:
        typeOfOrigin = PathStruct.TypeOfOrigin.NON_OVERRIDE_FIXED;
        break;
      case ENUM:
        typeOfOrigin = PathStruct.TypeOfOrigin.NON_OVERRIDE_ENUM;
        break;
      default:
        // unreachable
        throw new IllegalArgumentException(
            "Invalid state: NonOverrideProperties found for schema type : " + dataSchema.getType());
    }
    return new PathStruct("",
                          dataSchema.getProperties().get(getAnnotationNamespace()),
                          typeOfOrigin,
                          pathOfOrigin,
                          dataSchema);
  }

  private List<PathStruct> constructNonOverridePathStructFromProperties(String matchedField,
                                                                        Object propertiesForPath,
                                                                        PathStruct.TypeOfOrigin typeOfOrigin,
                                                                        ArrayDeque<String> pathOfOrigin,
                                                                        Object sourceOfOrigin)
  {
    // propertiesForPath has been null-checked, no other checks needed.
    PathStruct pathStruct = new PathStruct("",propertiesForPath, typeOfOrigin, pathOfOrigin, sourceOfOrigin);
    pathStruct.setMatchedPaths(new ArrayDeque<>(Arrays.asList(matchedField)));
    pathStruct.setPropertiesForPath(propertiesForPath);
    return new ArrayList<>(Arrays.asList(pathStruct));
  }

  @SuppressWarnings("unchecked")
  private List<PathStruct> constructOverridePathStructFromProperties(Map<String, Object> schemaProperties,
                                                                     PathStruct.TypeOfOrigin typeOfOrigin,
                                                                     ArrayDeque<String> pathOfOrigin,
                                                                     Object sourceOfOrigin,
                                                                     String startSchemaName)
  {
    Object properties = schemaProperties.getOrDefault(getAnnotationNamespace(), Collections.emptyMap());
    if (!(properties instanceof Map))
    {
      getVisitorTraversalResult().addMessage(pathOfOrigin, "Overrides entries should be key-value pairs that form a map");
      return new ArrayList<>();
    }

    // Check mal-formated Keys
    List<String> malformatedKeys = ((Map<String, Object>) properties).keySet()
                                                                     .stream()
                                                                     .filter(entryKey -> !PathSpec.validatePathSpecString(entryKey))
                                                                     .collect(toList());
    for (String malformatedKey : malformatedKeys)
    {
      getVisitorTraversalResult().addMessage(pathOfOrigin, "MalFormated key as pathspec found: %s", malformatedKey);
    }

    List<PathStruct> pathStructToReturn = ((Map<String, Object>) properties).entrySet()
                                             .stream().filter(entry -> PathSpec.validatePathSpecString(entry.getKey())) // Filter out incorrect format
                                             .map(entry -> new PathStruct(entry.getKey(),
                                                                          entry.getValue(),
                                                                          typeOfOrigin,
                                                                          pathOfOrigin,
                                                                          sourceOfOrigin))
                                             .collect(toList());
    // This is override, need to set start schema name for cyclic referencing checking
    pathStructToReturn.forEach(pathStruct -> pathStruct.setStartSchemaName(startSchemaName));

    return pathStructToReturn;
  }

  public static boolean couldStoreResolvedPropertiesInSchema(DataSchema dataSchema)
  {
    // For these schemas, the properties should store annotation needed
    // No overrides, therefore need to parse the PathSpec
    return (dataSchema instanceof PrimitiveDataSchema)
           || (dataSchema.getType() == DataSchema.Type.FIXED)
           || (dataSchema.getType() == DataSchema.Type.ENUM);
  }

  /**
   * This function try to process the current dataSchema being visited inside the context and create a skeleton copy of it.
   * But if the current dataSchema has been already processed. Fetch the cached skeleton copy.
   *
   * @param context {@link TraverserContext} context that contains current data schema.
   * @param hasOverridesNotResolved a boolean to tell whether there are non-resolved overrides that will be resolved into the new schema
   * @return the new schema
   * @throws CloneNotSupportedException
   */
  private DataSchema createSchemaAndAttachToParent(TraverserContext context, boolean hasOverridesNotResolved) throws CloneNotSupportedException
  {
    PathSpecTraverseVisitorContext oldVisitorContext = (PathSpecTraverseVisitorContext) (context.getVisitorContext());
    DataSchema currentDataSchema = context.getCurrentSchema();
    CurrentSchemaEntryMode currentSchemaEntryMode = context.getCurrentSchemaEntryMode();
    // newSchema could be created as skeletonSchema, or fetched from cache if currentDataSchema has already been processed.
    DataSchema newSchema = null;
    // attach based on visitorContext's schema, need to create new fields or union members
    DataSchema lastVisitedDataSchemaInSchemaConstructed = oldVisitorContext.getLastVisitedDataSchemaInSchemaConstructed();
    assert(lastVisitedDataSchemaInSchemaConstructed != null);

    if (hasOverridesNotResolved)
    {
      // if there are overrides that not resolved, always build skeleton schema
      newSchema = CopySchemaUtil.buildSkeletonSchema(currentDataSchema);
    } else
    {
      if (_seenDataSchemaMapping.containsKey(currentDataSchema))
      {
        newSchema = _seenDataSchemaMapping.get(currentDataSchema);
      } else
      {
        newSchema = CopySchemaUtil.buildSkeletonSchema(currentDataSchema);
        _seenDataSchemaMapping.put(currentDataSchema, newSchema);
      }
    }

    switch (currentSchemaEntryMode)
    {
      case FIELD:
        assert (lastVisitedDataSchemaInSchemaConstructed.getType() == DataSchema.Type.RECORD);
        RecordDataSchema.Field enclosingField = context.getEnclosingField();
        RecordDataSchema.Field newField =
            CopySchemaUtil.copyField(enclosingField, newSchema, lastVisitedDataSchemaInSchemaConstructed);
        List<RecordDataSchema.Field> fields =
            new ArrayList<>(((RecordDataSchema) lastVisitedDataSchemaInSchemaConstructed).getFields());
        fields.add(newField);
        ((RecordDataSchema) lastVisitedDataSchemaInSchemaConstructed).setFields(fields, new StringBuilder());
        break;
      case MAP_KEY:
        assert (lastVisitedDataSchemaInSchemaConstructed.getType() == DataSchema.Type.MAP);
        MapDataSchema mapDataSchema = (MapDataSchema) lastVisitedDataSchemaInSchemaConstructed;
        mapDataSchema.setKey((StringDataSchema) newSchema);
        break;
      case MAP_VALUE:
        assert (lastVisitedDataSchemaInSchemaConstructed.getType() == DataSchema.Type.MAP);
        mapDataSchema = (MapDataSchema) lastVisitedDataSchemaInSchemaConstructed;
        mapDataSchema.setValues(newSchema);
        break;
      case ARRAY_VALUE:
        assert (lastVisitedDataSchemaInSchemaConstructed.getType() == DataSchema.Type.ARRAY);
        ArrayDataSchema arrayDataSchema = (ArrayDataSchema) lastVisitedDataSchemaInSchemaConstructed;
        arrayDataSchema.setItems(newSchema);
        break;
      case UNION_MEMBER:
        assert (lastVisitedDataSchemaInSchemaConstructed.getType() == DataSchema.Type.UNION);
        UnionDataSchema.Member enclosingUnionMember = context.getEnclosingUnionMember();
        UnionDataSchema.Member newUnionMember = CopySchemaUtil.copyUnionMember(enclosingUnionMember, newSchema);
        List<UnionDataSchema.Member> unionMembers =
            new ArrayList<>(((UnionDataSchema) lastVisitedDataSchemaInSchemaConstructed).getMembers());
        unionMembers.add(newUnionMember);
        ((UnionDataSchema) lastVisitedDataSchemaInSchemaConstructed).setMembers(unionMembers, new StringBuilder());
        break;
      case TYPEREF_REF:
        TyperefDataSchema typerefDataSchema = (TyperefDataSchema) lastVisitedDataSchemaInSchemaConstructed;
        typerefDataSchema.setReferencedType(newSchema);
        break;
      default:
        break;
    }
    return newSchema;
  }

  /**
   * This function will use {@link SchemaAnnotationHandler#resolve(List, SchemaAnnotationHandler.ResolutionMetaData)}
   * @param propertiesOverrides pathStruct list which contain overrides
   * @param pathSpecComponents components list of current pathSpec to the location where this resolution happens
   * @return
   */
  private Map<String, Object> resolvePathStruct(List<PathStruct> propertiesOverrides, ArrayDeque<String> pathSpecComponents)
  {
    List<Pair<String, Object>> propertiesOverridesPairs = propertiesOverrides.stream()
                                                                             .map(pathStruct -> new ImmutablePair<>(
                                                                                 pathStruct.getOriginalPathString(),
                                                                                 pathStruct.getPropertiesForPath()))
                                                                             .collect(toList());
    SchemaAnnotationHandler.ResolutionResult result =
        _handler.resolve(propertiesOverridesPairs, new SchemaAnnotationHandler.ResolutionMetaData());
    if (result.isError())
    {
      getVisitorTraversalResult().addMessage(pathSpecComponents,
                                             "Annotations override resolution failed in handlers for %s",
                                             getAnnotationNamespace());
      getVisitorTraversalResult().addMessages(pathSpecComponents, result.getMessages());
    }
    return result.getResolvedResult();
  }

  private String getAnnotationNamespace()
  {
    return _handler.getAnnotationNamespace();
  }

  /**
   * An implementation of {@link VisitorContext}
   * Will be passed to this {@link PathSpecBasedSchemaAnnotationVisitor} from {@link DataSchemaRichContextTraverser}
   * through {@link TraverserContext}
   *
   */
  static class PathSpecTraverseVisitorContext implements VisitorContext
  {
    List<PathStruct> getLastLayerOverridesPathStructs()
    {
      return _lastLayerOverridesPathStructs;
    }

    void setLastLayerOverridesPathStructs(List<PathStruct> lastLayerOverridesPathStructs)
    {
      _lastLayerOverridesPathStructs = lastLayerOverridesPathStructs;
    }

    public DataSchema getLastVisitedDataSchemaInSchemaConstructed()
    {
      return _lastVisitedDataSchemaInSchemaConstructed;
    }

    public void setLastVisitedDataSchemaInSchemaConstructed(DataSchema lastVisitedDataSchemaInSchemaConstructed)
    {
      _lastVisitedDataSchemaInSchemaConstructed = lastVisitedDataSchemaInSchemaConstructed;
    }

    /**
     * Stores unresolved {@link PathStruct} from last layer recursion.
     */
    private List<PathStruct> _lastLayerOverridesPathStructs = new ArrayList<>();
    /**
     * Stores pointer to the last visited data schema for this particular visitorContext
     *
     * This is the actual last visited data schema that {@link PathSpecBasedSchemaAnnotationVisitor} built as part of {@link #_schemaConstructed}
     * within this visitorContext
     *
     */
    private DataSchema _lastVisitedDataSchemaInSchemaConstructed = null;
  }
}
