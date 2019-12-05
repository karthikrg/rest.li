package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.EnumDataSchema;
import com.linkedin.data.schema.FixedDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;


/**
 * PathStruct is a class that generated from
 * (1) an annotation override entry with PathSpec string as key and override properties as the value.
 * or (2) an annotation properties directly
 *
 * Take a schema example:
 *
 * <pre>{@code
 * record outerRcd{
 *    @customAnnotation= {"/f1/f2" : "2rd layer" }
 *    f: record rcd {
 *           @customAnnotation= {"/f2" : "1rd layer" }
 *           f1: record rcd2 {
 *               @customAnnotation = "OriginalValue"
 *               f2: string
 *           }
 *       }
 *    }
 * }
 * </pre>
 *
 * The pathStruct could be generated from
 * (1) {@code
 *        @customAnnotation= {"/f1/f2" : "2rd layer" }
 *     }
 *     _originalPathString is "/f1/f2"
 *     _propertiesForPath is "2rd layer"
 *     _pathOfOrigin is ["f"] (since this pathStruct is constructed from field "/f" counting from "outerRcd")
 *     _sourceOfOrigin is the RecordDataSchema named "outerRcd"
 *     _typeOfOrigin is OVERRIDE_RECORD_FIELD
 *     _matchedPaths is empty
 *     _remainingPaths is ["f1", "f2"] (because there are two segments need to be matched)
 *
 * (2) or @customAnnotation = "OriginalValue"
 *     _originalPathString is "" (no pathSpec specified)
 *     _propertiesForPath is "OriginalValue"
 *     _pathOfOrigin is ["f", "f1", "f2] (since this pathStruct is constructed from field "/f/f1/f2" counting from "outerRcd")
 *     _sourceOfOrigin is the RecordDataSchema named "rcd2"
 *     _typeOfOrigin is NON_OVERRIDE_RECORD_FIELD
 *     _matchedPaths is ["f2"]  (already matched field "f2")
 *     _remainingPaths is [] (because there are two segments need to be matched)
 *
 * In order to construct the PathStruct, a valid PathSpec string is assumed to be passed as an argument.
 * The constructor will parse the string into pathComponent, which are string segments separated by {@link PathSpec#SEPARATOR}
 * _remainingPaths and _matchedPaths could be changed dynamically when {@link PathSpecBasedSchemaAnnotationVisitor} is visiting the schema.
 *
 * _matchedPaths: path segments that have been matched
 * _remainingPaths: path segments that have not been matched
 * _propertiesForPath: the properties that this pathStruct is constructed from
 * _startSchemaName: the start schema Name that this pathStruct is constructed from
 *
 * As previously explained by the example, in detail, the PathStruct can be generated from two ways:
 * (1) For overrides:
 *  (1.1). From {@link RecordDataSchema.Field}
 *  (1.2). From {@link TyperefDataSchema}
 *  (1.3). From {@link RecordDataSchema}: RecordDataSchemas' properties can have overrides for "included" RecordDataSchema.
 *
 * (2) For non-override:
 *  (2.1). From named schema: {@link TyperefDataSchema}
 *  (2.2). From named schema: {@link EnumDataSchema}
 *  (2.3). From named schema: {@link FixedDataSchema}
 *  (2.4). From {@link RecordDataSchema.Field}
 *
 */
public class PathStruct
{
  // path segments that have been matched
  private ArrayDeque<String> _matchedPaths = new ArrayDeque<>();
  // path segments that have not been matched
  private ArrayDeque<String> _remainingPaths = new ArrayDeque<>();
  // the actual property value that this pathStruct stored
  private Object _propertiesForPath;
  // the original PathSpec string
  private final String _originalPathString;
  // The traverser path of the Field/DataSchema that this PathStruct constructed from
  private final ArrayDeque<String> _pathOfOrigin;
  // The source DataSchema/Field that this pathStruct constructed from
  private final Object _sourceOfOrigin;
  /**
   * The type of this PathSpec is originated from, also see {@link TypeOfOrigin}
   */
  private final TypeOfOrigin _typeOfOrigin;
  // This field is used for Cyclic overriding detection. Need to record the start Schema that this PathStruct
  // is generated, and when a next path segment to match has the same name as this startSchemaName, we detect the
  // cyclic referencing
  private String _startSchemaName = "";

  /**
   * This is the attribute to tell whether this {@link PathStruct}'s override path has been validated.
   *
   */
  private OverridePathValidStatus _overridePathValidStatus = OverridePathValidStatus.UNCHECKED;

  enum OverridePathValidStatus
  {
    UNCHECKED,
    VALID,
    INVALID,
  }

  public OverridePathValidStatus getOverridePathValidStatus()
  {
    return _overridePathValidStatus;
  }

  public void setOverridePathValidStatus(OverridePathValidStatus overridePathValidStatus)
  {
    _overridePathValidStatus = overridePathValidStatus;
  }

  enum TypeOfOrigin
  {
    OVERRIDE_RECORD_FIELD,
    OVERRIDE_RECORD_FOR_INCLUDE,
    OVERRIDE_TYPE_REF_OVERRIDE,
    NON_OVERRIDE_TYPE_REF,
    NON_OVERRIDE_ENUM,
    NON_OVERRIDE_FIXED,
    NON_OVERRIDE_RECORD_FIELD
  }

  PathStruct(String pathSpecStr,
             Object propertiesForPath,
             TypeOfOrigin typeOfOrigin,
             ArrayDeque<String> pathOfOrigin,
             Object sourceOfOrigin)
  {
    _remainingPaths = new ArrayDeque<>(Arrays.asList(pathSpecStr.split(Character.toString(PathSpec.SEPARATOR))));
    _remainingPaths.remove("");
    _propertiesForPath = propertiesForPath;
    _originalPathString = pathSpecStr;
    _typeOfOrigin = typeOfOrigin;
    _pathOfOrigin = new ArrayDeque<>(pathOfOrigin);
    _sourceOfOrigin  = sourceOfOrigin;
  }

  //Deep copy
  static PathStruct copyOf(PathStruct another)
  {
    PathStruct pathStruct = new PathStruct(another.getOriginalPathString(),
                                           another.getPropertiesForPath(),
                                           another.getTypeOfOrigin(),
                                           another.getPathOfOrigin(),
                                           another.getSourceOfOrigin());
    pathStruct.setMatchedPaths(new ArrayDeque<>(another.getMatchedPaths()));
    pathStruct.setRemainingPaths(new ArrayDeque<>(another.getRemainingPaths()));
    return pathStruct;
  }

  boolean isOverride()
  {
    return new HashSet<>(Arrays.asList(TypeOfOrigin.OVERRIDE_RECORD_FIELD,
                                       TypeOfOrigin.OVERRIDE_RECORD_FOR_INCLUDE,
                                       TypeOfOrigin.OVERRIDE_TYPE_REF_OVERRIDE)).contains(_typeOfOrigin);
  }

  ArrayDeque<String> getMatchedPaths()
  {
    return _matchedPaths;
  }

  ArrayDeque<String> getRemainingPaths()
  {
    return _remainingPaths;
  }

  Object getPropertiesForPath()
  {
    return _propertiesForPath;
  }

  void setMatchedPaths(ArrayDeque<String> matchedPaths)
  {
    this._matchedPaths = matchedPaths;
  }

  void setRemainingPaths(ArrayDeque<String> remainingPaths)
  {
    this._remainingPaths = remainingPaths;
  }

  void setPropertiesForPath(Object propertiesForPath)
  {
    this._propertiesForPath = propertiesForPath;
  }

  String getStartSchemaName()
  {
    return _startSchemaName;
  }

  void setStartSchemaName(String startSchemaName)
  {
    this._startSchemaName = startSchemaName;
  }

  public String getOriginalPathString()
  {
    return _originalPathString;
  }

  public TypeOfOrigin getTypeOfOrigin()
  {
    return _typeOfOrigin;
  }


  public ArrayDeque<String> getPathOfOrigin()
  {
    return _pathOfOrigin;
  }

  public Object getSourceOfOrigin()
  {
    return _sourceOfOrigin;
  }
}
