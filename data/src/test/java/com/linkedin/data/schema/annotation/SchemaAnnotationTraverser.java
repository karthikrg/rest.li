package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

//TODO: modified so looks like a new file

/**
 * Principle in resolution: Lazy clone two reasons
 * (1) cyclic reference should be embedded by nature
 * (2) eventually do validation, just check all primitive fields
 *
 *
 */

class SchemaAnnotationTraverser
{

  enum AttachMode
  {
    FIELD,
    MAP_KEY,
    MAP_VALUE,
    ARRAY_VALUE,
    UNION_MEMBER,
    TYPEREF_REF
  }

  private final IdentityHashMap<DataSchema, Boolean> _seen = new IdentityHashMap<DataSchema, Boolean>();
  protected final ArrayList<String> _path = new ArrayList<String>();
  private final ArrayList<AttachMode> _attachModeStack = new ArrayList<>();

  //Ensure smae patshpac doesn't appear twice
//  private final ArrayList<DataSchema> _seenDataSchema = new ArrayList<>();
  private final IdentityHashMap<DataSchema, Boolean > _seenDataSchema = new IdentityHashMap<>();
  protected final ArrayList<String> _pathSpec = new ArrayList<String>();

  public SchemaAnnotationTraverser()
  {
  }

  public void traverse(DataSchema schema)
  {
    _seen.clear();
    _path.clear();

    _seen.put(schema, Boolean.TRUE);
    onRecursion(schema, null, null, null);
    assert (_path.isEmpty());
  }

  // Start the recursion on next component
  protected void onRecursion(DataSchema schema, RecordDataSchema.Field enclosingField, DataSchema parentDataSchema, UnionDataSchema.Member unionMember)
  {
    if (schema instanceof NamedDataSchema)
    {
      _path.add(((NamedDataSchema) schema).getFullName());
    } else
    {
      _path.add(schema.getUnionMemberKey());
    }

    //Pre-order
    schema = visitComponent(_pathSpec, schema, enclosingField, currentOverrides, parentDataSchema, unionMember);

//    if (!(new HashSet<>(_seenDataSchema).contains(schema)))
//    if (_seenDataSchema.stream().noneMatch(seenSchema -> seenSchema == finalSchema))
    if (!_seenDataSchema.containsKey(schema))
    {
      _seenDataSchema.put(schema, Boolean.TRUE);

      switch (schema.getType())
      {
        case TYPEREF:
          TyperefDataSchema typerefDataSchema = (TyperefDataSchema) schema;
          _attachModeStack.add(AttachMode.TYPEREF_REF);
          traverseChild(DataSchemaConstants.REF_KEY, typerefDataSchema.getRef(), enclosingField, ((TyperefDataSchema) schema).getFullName(), schema, null);
          _attachModeStack.remove(_attachModeStack.size() - 1);
          break;
        case MAP:
          //traverse key if has matched
          MapDataSchema mapDataSchema = (MapDataSchema) schema;
          //then traverse values
          _attachModeStack.add(AttachMode.MAP_KEY);
          traverseChild(DataSchemaConstants.MAP_KEY_REF, mapDataSchema.getKey(), enclosingField,
                        DataSchemaConstants.MAP_KEY_REF, schema, null);
          _attachModeStack.remove(_attachModeStack.size() - 1);

          _attachModeStack.add(AttachMode.MAP_VALUE);
          traverseChild(PathSpec.WILDCARD, mapDataSchema.getValues(), enclosingField, PathSpec.WILDCARD, schema, null);
          _attachModeStack.remove(_attachModeStack.size() - 1);
          break;
        case ARRAY:
          ArrayDataSchema arrayDataSchema = (ArrayDataSchema) schema;
          _attachModeStack.add(AttachMode.ARRAY_VALUE);
          traverseChild(PathSpec.WILDCARD, arrayDataSchema.getItems(), enclosingField, PathSpec.WILDCARD, schema, null);
          _attachModeStack.remove(_attachModeStack.size() - 1);
          break;
        case RECORD:
          RecordDataSchema recordDataSchema = (RecordDataSchema) schema;
          for (RecordDataSchema.Field field : recordDataSchema.getFields())
          {

            _attachModeStack.add(AttachMode.FIELD);
            traverseChild(field.getName(), field.getType(), field, field.getName(), schema, null);
            _attachModeStack.remove(_attachModeStack.size() - 1);

          }
          break;
        case UNION:
          UnionDataSchema unionDataSchema = (UnionDataSchema) schema;
          for (UnionDataSchema.Member member : unionDataSchema.getMembers())
          {
            _attachModeStack.add(AttachMode.UNION_MEMBER);
            traverseChild(member.getUnionMemberKey(), member.getType(), enclosingField, member.getUnionMemberKey(), schema, member);
            _attachModeStack.remove(_attachModeStack.size() - 1);
          }
          break;
        case FIXED:
          // treated similar to Primitive
          break;
        case ENUM:
          // treated similar to Primitive
          //TODO: Still need to access enum to see resolvedProperties
          //TODO: enum might have both field and
          break;
        default:
          assert (schema.isPrimitive());
          break;
      }
      _seenDataSchema.remove(_seenDataSchema.size() - 1);
    }

    //Post-order
    // if (_callbacks.containsKey(Order.POST_ORDER))
    // {
    //   _callbacks.get(Order.POST_ORDER).visitComponent(_path, schema, enclosingField, currentOverrides);
    // }

    _path.remove(_path.size() - 1);
  }

  @SuppressWarnings(value="unchecked")
  private List<PathStruct> generateOverridePathStructFromInclude(RecordDataSchema dataSchema)
  {
    //Include can only be overrides
    Map<String, Object> properties =
        (Map<String, Object>) dataSchema.getProperties().getOrDefault(getAnnotationNameSpace(), Collections.emptyMap());
    List<PathStruct> pathStructToReturn = properties.entrySet()
                                                    .stream()
                                                    .map(entry -> new PathStruct(entry.getKey(), entry.getValue()))
                                                    .collect(toList());
    // This is override, need to set start schema name for cyclic referencing checking
    pathStructToReturn.forEach(pathStruct -> pathStruct.setStartSchemaName(dataSchema.getFullName()));
    return pathStructToReturn;
  }


  @SuppressWarnings(value="unchecked")
  private List<PathStruct> generateOverridePathStructFromField(RecordDataSchema.Field field)
  {
    if (!(couldStoreResolvedPropertiesInSchema(field.getType())))
    {
      // Overrides could only happen if the field's schema could not store resolvedProperties directly
      //TODO: Consider: error handling for casting, should add to error if the format is incorrect
      //Must be a map
      Map<String, Object> properties =
          (Map<String, Object>) field.getProperties().getOrDefault(getAnnotationNameSpace(), Collections.emptyMap());
      List<PathStruct> pathStructToReturn = properties.entrySet()
                       .stream()
                       .map(entry -> new PathStruct(entry.getKey(), entry.getValue()))
                       .collect(toList());
      // This is override, need to set start schema name for cyclic referencing checking
      pathStructToReturn.forEach(pathStruct -> pathStruct.setStartSchemaName(field.getRecord().getFullName()));
      return pathStructToReturn;
    } else
    {
      if (field.getProperties().get(annotationNameSpace) != null)
      {
        PathStruct pathStruct = new PathStruct();
        pathStruct.setMatchedPaths(Arrays.asList(field.getName()));
        // TODO: Consider: here also need to check if the content is correct?
        pathStruct.setPropertiesForPath(field.getProperties().get(annotationNameSpace));
        return Arrays.asList(pathStruct);
      }
      return new ArrayList<>();
    }
  }

  @SuppressWarnings(value="unchecked")
  private List<PathStruct> generateOverridePathStructFromTypeRefSchema(TyperefDataSchema dataSchema)
  {
//    TyperefDataSchema typerefDataSchema = (TyperefDataSchema) dataSchema;
    List<PathStruct> typerRefPathStructs = new ArrayList<>();
    if (!couldStoreResolvedPropertiesInSchema(dataSchema.getDereferencedDataSchema()))
    {
      //Should treat as overriding
        Map<String, Object> properties =
            (Map<String, Object>) dataSchema.getProperties().getOrDefault(getAnnotationNameSpace(), Collections.emptyMap());
        typerRefPathStructs.addAll(properties.entrySet()
                                             .stream()
                                             .map(entry -> new PathStruct(entry.getKey(),
                                                                          entry.getValue(),
                                                                          true))
                                             .collect(toList()));
      typerRefPathStructs.forEach(pathStruct -> pathStruct.getMatchedPaths().add(dataSchema.getFullName()));
    } else
    {
        if (dataSchema.getProperties().get(annotationNameSpace) != null)
        {
          PathStruct pathStruct = new PathStruct();
          pathStruct.setFromTypeRef(true);
          pathStruct.setPropertiesForPath(dataSchema.getProperties().get(annotationNameSpace));
          pathStruct.setMatchedPaths(Arrays.asList(dataSchema.getFullName()));
          typerRefPathStructs.add(pathStruct);
        }
    }

    return typerRefPathStructs;
  }

  private boolean couldStoreResolvedPropertiesInSchema(DataSchema dataSchema)
  {
    return (dataSchema instanceof PrimitiveDataSchema)
           || (dataSchema.getType() == DataSchema.Type.FIXED)
           || (dataSchema.getType() == DataSchema.Type.ENUM);

  }

  //before start next recursion
  //this function handles parent-child relation between parent and child schemas.
  protected void traverseChild(String childKey,
                               DataSchema childSchema,
                               RecordDataSchema.Field enclosingField,
                               String pathSpecMatchingSegment,
                               DataSchema parentSchema,
                               UnionDataSchema.Member member)
  {
//    if (! _seen.containsKey(childSchema))
//    {
    //TODO: if field schema is primitive, resolve to it right now

    _seen.put(childSchema, Boolean.TRUE);
    _path.add(childKey);
    if (parentSchema.getType() != DataSchema.Type.TYPEREF)
    {
      // PathSpec shouldn't include TypeRef
      _pathSpec.add(childKey);
    }

    Stream<PathStruct> lastLayerStream = null;
    lastLayerStream = pathStack.get(pathStack.size() - 1).stream();
    if (parentSchema.getType() == DataSchema.Type.RECORD && ((RecordDataSchema)parentSchema).getInclude().size() > 0)
    {
      //Process includes
      lastLayerStream = Stream.concat(lastLayerStream, generateOverridePathStructFromInclude((RecordDataSchema) parentSchema).stream());
    }
    currentOverrides = lastLayerStream.filter(pathStruct -> pathStruct.getRemainingPaths()
                                                                      .get(pathStruct.getRemainingPaths().size() - 1)
                                                                      .equals(pathSpecMatchingSegment))
                                      .map(PathStruct::copyOf)
                                      .collect(toList());
    currentOverrides.forEach(pathStruct ->
                             {
                               pathStruct.getMatchedPaths().add(pathSpecMatchingSegment);
                               pathStruct.getRemainingPaths().remove(pathStruct.getRemainingPaths().size() - 1);
                             });
    switch (parentSchema.getType())
    {
      case RECORD:
        currentOverrides.addAll(generateOverridePathStructFromField(enclosingField));
        break;
      case TYPEREF:
        currentOverrides.addAll(generateOverridePathStructFromTypeRefSchema((TyperefDataSchema) parentSchema));
        break;
      default:
        break;
    }

    if (childSchema.getType() == DataSchema.Type.TYPEREF)
    {
      // Add child TypeRef full schema name to all currentOverrides
      // So they can be recognized when visiting the child TypeRef
      currentOverrides.forEach(pathStruct -> pathStruct.getRemainingPaths().add(((TyperefDataSchema)childSchema).getFullName()));
    }

    //Below is for cyclic referencing checking:
    //after merging the override paths from this field
    //need to check whether this is producing cyclic overriding
    //If filtered currentOverrides going to pass to a childSchema that is same name as the start schema in a pathStruct
    //  then it is invalid
    //Note: Cyclic referencing in TypeRef is handled in the similar way, deferenced Schema of parent schema is the childSchema here
    if (childSchema.getType() == DataSchema.Type.RECORD &&
        currentOverrides.stream()
                        .anyMatch(pathStruct ->
                                      pathStruct.getStartSchemaName()
                                                .equals(((RecordDataSchema) childSchema).getFullName())))
    {
      System.out.println("Error: seen cyclic referencing");
      _path.remove(_path.size() - 1);
      _pathSpec.remove(_pathSpec.size() - 1);
      return;
    }
    pathStack.add(new ArrayList<>(currentOverrides));
    onRecursion(childSchema, enclosingField, parentSchema, member);
    pathStack.remove(pathStack.size() - 1);
    _path.remove(_path.size() - 1);

    if (parentSchema.getType() != DataSchema.Type.TYPEREF)
    {
      _pathSpec.remove(_pathSpec.size() - 1);
    }
//    }
  }

  @SuppressWarnings(value="serial")
  public Map<String, Object> someResolveLogic(List<PathStruct> propertiesOverrides)
  {
    //TODO: check whether all overrides are valid
    if (propertiesOverrides.get(0).getPropertiesForPath() == null)
    {
      //This is actually an error case
      return Collections.emptyMap();
    }

    return new HashMap<String, Object>()
    {{
      put(getAnnotationNameSpace(), propertiesOverrides.get(0).getPropertiesForPath());
    }};
  }

  /**
   * Operates on component, which is the schema under recursion
   * Will make copy based on whether overrides are present(mostly does the copy logic)
   * Then attach to related
   *
   * @param path
   * @param schema
   * @param enclosingField
   * @param currentOverrides
   * @return
   */
  public DataSchema visitComponent(List<String> path,
                                   DataSchema schema,
                                   RecordDataSchema.Field enclosingField,
                                   List<PathStruct> currentOverrides,
                                   DataSchema parentDataSchema,
                                   UnionDataSchema.Member unionMember)
  {
    try
    {
      //TODO: need to push the resolvedProperties to the primitive types so can be printed out
      if (couldStoreResolvedPropertiesInSchema(schema))
      {
        // For enum and fixed, the properties can be written to the schema itself, so need to add them to currentOverrides
        if (schema.getProperties().get(getAnnotationNameSpace()) != null)
        {
          currentOverrides.add(new PathStruct("", schema.getProperties().get(getAnnotationNameSpace())));
        }


        //TODO: use actual resolve instead of directly getting from enclosingField
        if (currentOverrides.size() != 0)
        { //equivalently: if calculated overrides AND seen properties needs to be propagated
          schema= getSchemaCopy(schema);
          attach(schema, enclosingField, unionMember, parentDataSchema, _attachModeStack.get(_attachModeStack.size() - 1));

          //TODO: use actual resolve instead of directly getting from enclosingField
          // TODO: for ENUM and Fixed, need to include schema properties
          schema.getResolvedProperties().putAll(someResolveLogic(currentOverrides)); // Actually use resolve rules here
        }

        // Below is for printing
        Map<String, Object> resolvedProperties = schema.getResolvedProperties();
        String mapStringified =
            resolvedProperties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining("&"));
        System.out.println(String.format("/%s ::: %s", String.join("/", path), mapStringified));
      } else if (schema.isComplex())
      {
        // TODO: Should consider fixed
        //


        // if current layer has more than one overrides
        // seems that should only care about overrides, but not the schema itself
        // typeRef is still a headache
        if (currentOverrides.size() > 0)
        {
          switch (schema.getType())
          {
            case RECORD:
            case UNION:
            case TYPEREF:
            case ARRAY:
              schema =  getSchemaCopy(schema);
              break;
            case MAP:
              //In map, key field are newed, which made the MapDataSchema a new copy
            default:
              break;
          }
          attach(schema, enclosingField, unionMember, parentDataSchema, _attachModeStack.get(_attachModeStack.size() - 1));
        }
      }
      return schema;
    } catch (Exception e)
    {
      return null;
    }
  }

  protected void attach(DataSchema schema,
                      RecordDataSchema.Field enclosingField,
                      UnionDataSchema.Member unionMember,
                      DataSchema parentDataSchema,
                      AttachMode attachMode)
  {
    switch (attachMode)
    {
      case FIELD:
        enclosingField.setType(schema);
        break;
      case MAP_KEY:
        MapDataSchema mapDataSchema = (MapDataSchema) parentDataSchema;
        mapDataSchema.setKey((StringDataSchema) schema);
        break;
      case MAP_VALUE:
        mapDataSchema = (MapDataSchema) parentDataSchema;
        mapDataSchema.setValues(schema);
        break;
      case ARRAY_VALUE:
        ArrayDataSchema arrayDataSchema = (ArrayDataSchema) parentDataSchema;
        arrayDataSchema.setItems(schema);
        break;
      case UNION_MEMBER:
        unionMember.setType(schema);
        break;
      case TYPEREF_REF:
        TyperefDataSchema typerefDataSchema = (TyperefDataSchema) parentDataSchema;
        typerefDataSchema.setReferencedType(schema);
        break;
      default:
        break;
    }

  }

  static private DataSchema getSchemaCopy(DataSchema schema) throws CloneNotSupportedException
  {
    DataSchema newSchema = null;
    switch (schema.getType())
    {
      case RECORD:
        // shallow copy the recordDataSchema, but also shallow copy the fields.
        newSchema = schema.clone();
        List<RecordDataSchema.Field> fields = ((RecordDataSchema) newSchema).getFields();
        List<RecordDataSchema.Field> newFields = fields.stream().map(field ->
                                                                     {
                                                                       try
                                                                       {
                                                                         return field.clone();
                                                                       } catch (Exception e)
                                                                       {
                                                                         // TODO: exception handling
                                                                       }
                                                                       return null;
                                                                     }).collect(toList());
        ((RecordDataSchema) newSchema).setFields(newFields, new StringBuilder());
        break;
      case UNION:
        // shallow copy UnionDataSchema, but also shallow copy the members
        newSchema = schema.clone();
        List<UnionDataSchema.Member> members = ((UnionDataSchema) newSchema).getMembers();
        List<UnionDataSchema.Member> newMembers = members.stream().map(member ->
                                                                        {
                                                                          try
                                                                          {
                                                                            return member.clone();
                                                                          } catch (Exception e)
                                                                          {
                                                                            //TODO
                                                                          }
                                                                          return null;
                                                                        }
                                                                      ).collect(toList());
        ((UnionDataSchema) newSchema).setMembers(newMembers, new StringBuilder());
        break;
      case TYPEREF:
      case FIXED:
      case ENUM:
      case ARRAY:
        newSchema = schema.clone();
        break;
      case MAP:
        // because the key field already made sure each map data schema would be separate copy

        break;
      default:
        assert (schema.isPrimitive());
        newSchema = schema.clone();
        break;
    }
    return newSchema;
  }

  private StringBuilder _errorMessageBuilder = new StringBuilder();

  public static String getAnnotationNameSpace()
  {
    return annotationNameSpace;
  }

  private static final String annotationNameSpace = "compliance"; //TODO: fix this
  @SuppressWarnings(value="serial")
  private List<List<PathStruct>> pathStack = new ArrayList<List<PathStruct>>()
  {{
    add(new ArrayList<>());
  }};
  private List<PathStruct> currentOverrides = new ArrayList<>();


  public static class PathStruct
  {
    List<String> matchedPaths = new ArrayList<>();
    List<String> remainingPaths = new ArrayList<>();
    Object propertiesForPath;
    String startSchemaName = "";

    boolean isFromTypeRef = false;

    public PathStruct()
    {
    }

    public PathStruct(String pathSpecStr, Object propertiesForPath)
    {
      remainingPaths = new ArrayList<>(Arrays.asList(pathSpecStr.split("/")));
      Collections.reverse(remainingPaths);
      remainingPaths.remove(remainingPaths.size() - 1);
      this.propertiesForPath = propertiesForPath;
    }

    public PathStruct(String pathSpecStr, Object propertiesForPath, boolean isFromTypeRef)
    {
      this(pathSpecStr, propertiesForPath);
      setFromTypeRef(isFromTypeRef);
    }

    //Deep copy
    public static PathStruct copyOf(PathStruct another)
    {
      PathStruct pathStruct = new PathStruct();
      pathStruct.setMatchedPaths(new ArrayList<>(another.getMatchedPaths()));
      pathStruct.setPropertiesForPath(another.getPropertiesForPath()); //original properties are used as immutable
      pathStruct.setRemainingPaths(new ArrayList<>(another.getRemainingPaths()));
      return pathStruct;
    }

    public List<String> getMatchedPaths()
    {
      return matchedPaths;
    }

    public List<String> getRemainingPaths()
    {
      return remainingPaths;
    }

    public Object getPropertiesForPath()
    {
      return propertiesForPath;
    }

    public void setMatchedPaths(List<String> matchedPaths)
    {
      this.matchedPaths = matchedPaths;
    }

    public void setRemainingPaths(List<String> remainingPaths)
    {
      this.remainingPaths = remainingPaths;
    }

    public void setPropertiesForPath(Object propertiesForPath)
    {
      this.propertiesForPath = propertiesForPath;
    }

    public String getStartSchemaName()
    {
      return startSchemaName;
    }

    public void setStartSchemaName(String startSchemaName)
    {
      this.startSchemaName = startSchemaName;
    }

    public boolean isFromTypeRef()
    {
      return isFromTypeRef;
    }

    public void setFromTypeRef(boolean fromTypeRef)
    {
      isFromTypeRef = fromTypeRef;
    }

  }
}

