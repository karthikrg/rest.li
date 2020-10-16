/*
   Copyright (c) 2015 LinkedIn Corp.

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

package com.linkedin.pegasus.generator;

import com.linkedin.data.ByteString;
import com.linkedin.data.Data;
import com.linkedin.data.DataMap;
import com.linkedin.data.collections.CheckedMap;
import com.linkedin.data.collections.SpecificDataComplexProvider;
import com.linkedin.data.collections.SpecificDataTemplateSchemaMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.schema.EnumDataSchema;
import com.linkedin.data.schema.JsonBuilder;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.SchemaFormatType;
import com.linkedin.data.schema.SchemaToJsonEncoder;
import com.linkedin.data.schema.SchemaToPdlEncoder;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.BooleanArray;
import com.linkedin.data.template.BooleanMap;
import com.linkedin.data.template.BytesArray;
import com.linkedin.data.template.BytesMap;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.DirectArrayTemplate;
import com.linkedin.data.template.DirectMapTemplate;
import com.linkedin.data.template.DoubleArray;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.FixedTemplate;
import com.linkedin.data.template.FloatArray;
import com.linkedin.data.template.FloatMap;
import com.linkedin.data.template.HasTyperefInfo;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.data.template.IntegerMap;
import com.linkedin.data.template.LongArray;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.TemplateOutputCastException;
import com.linkedin.data.template.TyperefInfo;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.data.template.WrappingArrayTemplate;
import com.linkedin.data.template.WrappingMapTemplate;
import com.linkedin.pegasus.generator.spec.ArrayTemplateSpec;
import com.linkedin.pegasus.generator.spec.ClassTemplateSpec;
import com.linkedin.pegasus.generator.spec.CustomInfoSpec;
import com.linkedin.pegasus.generator.spec.EnumTemplateSpec;
import com.linkedin.pegasus.generator.spec.FixedTemplateSpec;
import com.linkedin.pegasus.generator.spec.MapTemplateSpec;
import com.linkedin.pegasus.generator.spec.ModifierSpec;
import com.linkedin.pegasus.generator.spec.PrimitiveTemplateSpec;
import com.linkedin.pegasus.generator.spec.RecordTemplateSpec;
import com.linkedin.pegasus.generator.spec.TyperefTemplateSpec;
import com.linkedin.pegasus.generator.spec.UnionTemplateSpec;
import com.linkedin.util.ArgumentUtil;
import com.sun.codemodel.ClassType;
import com.sun.codemodel.JAnnotatable;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCase;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JClassContainer;
import com.sun.codemodel.JCommentPart;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JDocCommentable;
import com.sun.codemodel.JEnumConstant;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JOp;
import com.sun.codemodel.JSwitch;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generates CodeModel {@link JClass} of data templates from {@link ClassTemplateSpec}.
 *
 * @author Keren Jin
 */
public class JavaDataTemplateGenerator extends JavaCodeGeneratorBase
{
  /**
   * Rest.li pre-defines some commonly used Java data template classes such as {@link IntegerArray}.
   * This generator will directly use these classes instead of generate them anew.
   */
  public static final Map<DataSchema, Class<?>> PredefinedJavaClasses;
  static
  {
    final Class<?>[] predefinedClass = new Class<?>[] {
        BooleanArray.class,
        BooleanMap.class,
        BytesArray.class,
        BytesMap.class,
        DoubleArray.class,
        DoubleMap.class,
        FloatArray.class,
        FloatMap.class,
        IntegerArray.class,
        IntegerMap.class,
        LongArray.class,
        LongMap.class,
        StringArray.class,
        StringMap.class
    };

    PredefinedJavaClasses = new HashMap<DataSchema, Class<?>>();

    for (Class<?> clazz : predefinedClass)
    {
      final DataSchema schema = DataTemplateUtil.getSchema(clazz);
      PredefinedJavaClasses.put(schema, clazz);
    }
  }

  /*
   * When the original schema format type cannot be determined, encode the generated schema field in this format.
   * This is primarily to handle data templates generated from IDLs, which are not in a particular schema format.
   *
   * TODO: once the PDL migration is done, switch this to PDL
   */
  private static final SchemaFormatType DEFAULT_SCHEMA_FORMAT_TYPE = SchemaFormatType.PDSC;

  private static final int DEFAULT_DATAMAP_INITIAL_CAPACITY = 16; // From HashMap's default initial capacity
  private static final Logger _log = LoggerFactory.getLogger(JavaDataTemplateGenerator.class);
  //
  // Deprecated annotation utils
  //
  private static final String DEPRECATED_KEY = "deprecated";

  private final Map<ClassTemplateSpec, JDefinedClass> _definedClasses = new HashMap<ClassTemplateSpec, JDefinedClass>();
  private final Map<JDefinedClass, ClassTemplateSpec> _generatedClasses = new HashMap<JDefinedClass, ClassTemplateSpec>();

  private final JClass _recordBaseClass;
  private final JClass _unionBaseClass;
  private final JClass _wrappingArrayBaseClass;
  private final JClass _wrappingMapBaseClass;
  private final JClass _directArrayBaseClass;
  private final JClass _directMapBaseClass;
  private final JClass _schemaFormatTypeClass;

  private final boolean _recordFieldAccessorWithMode;
  private final boolean _recordFieldRemove;
  private final boolean _pathSpecMethods;
  private final boolean _copierMethods;
  private final String _rootPath;

  private JavaDataTemplateGenerator(String defaultPackage,
                                    boolean recordFieldAccessorWithMode,
                                    boolean recordFieldRemove,
                                    boolean pathSpecMethods,
                                    boolean copierMethods,
                                    String rootPath)
  {
    super(defaultPackage);

    _recordBaseClass = getCodeModel().ref(RecordTemplate.class);
    _unionBaseClass = getCodeModel().ref(UnionTemplate.class);
    _wrappingArrayBaseClass = getCodeModel().ref(WrappingArrayTemplate.class);
    _wrappingMapBaseClass = getCodeModel().ref(WrappingMapTemplate.class);
    _directArrayBaseClass = getCodeModel().ref(DirectArrayTemplate.class);
    _directMapBaseClass = getCodeModel().ref(DirectMapTemplate.class);
    _schemaFormatTypeClass = getCodeModel().ref(SchemaFormatType.class);

    _recordFieldAccessorWithMode = recordFieldAccessorWithMode;
    _recordFieldRemove = recordFieldRemove;
    _pathSpecMethods = pathSpecMethods;
    _copierMethods = copierMethods;
    _rootPath = rootPath;
  }

  public JavaDataTemplateGenerator(Config config)
  {
    this(config.getDefaultPackage(),
         config.getRecordFieldAccessorWithMode(),
         config.getRecordFieldRemove(),
         config.getPathSpecMethods(),
         config.getCopierMethods(),
         config.getRootPath());
  }

  /**
   * @param defaultPackage package to be used when a {@link NamedDataSchema} does not specify a namespace
   */
  public JavaDataTemplateGenerator(String defaultPackage)
  {
    this(defaultPackage,
         null);
  }

  /**
   * @param defaultPackage package to be used when a {@link NamedDataSchema} does not specify a namespace
   * @param rootPath root path to relativize the location
   */
  public JavaDataTemplateGenerator(String defaultPackage, String rootPath)
  {
    this(defaultPackage,
         true,
         true,
         true,
         true,
         rootPath);
  }

  public Map<JDefinedClass, ClassTemplateSpec> getGeneratedClasses()
  {
    return _generatedClasses;
  }

  public JClass generate(ClassTemplateSpec classTemplateSpec)
  {
    final JClass result;

    if (classTemplateSpec == null)
    {
      result = null;
    }
    else
    {
      if (classTemplateSpec.getSchema() == null)
      {
        // this is for custom class, package override is not applicable.
        result = getCodeModel().directClass(classTemplateSpec.getFullName());
      }
      else if (PredefinedJavaClasses.containsKey(classTemplateSpec.getSchema()))
      {
        final Class<?> nativeJavaClass = PredefinedJavaClasses.get(classTemplateSpec.getSchema());
        result = getCodeModel().ref(nativeJavaClass);
      }
      else if (classTemplateSpec.getSchema().isPrimitive())
      {
        result = generatePrimitive((PrimitiveTemplateSpec) classTemplateSpec);
      }
      else
      {
        try
        {
          final JDefinedClass definedClass = defineClass(classTemplateSpec);
          populateClassContent(classTemplateSpec, definedClass);
          result = definedClass;
        }
        catch (JClassAlreadyExistsException e)
        {
          throw new IllegalArgumentException(classTemplateSpec.getFullName());
        }
      }
    }

    return result;
  }

  private static JInvocation dataClassArg(JInvocation inv, JClass dataClass)
  {
    if (dataClass != null)
    {
      inv.arg(JExpr.dotclass(dataClass));
    }
    return inv;
  }

  private void generateCopierMethods(JDefinedClass templateClass, Map<String, JVar> fields,
      Map<String, JVar> hasFields, JClass changeListenerClass, JVar specificMapVar)
  {
    // Clone is a shallow copy and shouldn't reset fields, copy is a deep copy and should.
    overrideCopierMethod(templateClass, "clone", fields, hasFields, false, changeListenerClass, specificMapVar);
    overrideCopierMethod(templateClass, "copy", fields, hasFields, true, changeListenerClass, specificMapVar);
  }

  private static boolean hasNestedFields(DataSchema schema)
  {
    while (true)
    {
      switch (schema.getDereferencedType())
      {
        case RECORD:
          return true;
        case UNION:
          return true;
        case ARRAY:
          schema = ((ArrayDataSchema) schema.getDereferencedDataSchema()).getItems();
          continue;
        case MAP:
          schema = ((MapDataSchema) schema.getDereferencedDataSchema()).getValues();
          continue;
        default:
          return false;
      }
    }
  }

  private static boolean isArrayType(DataSchema schema)
  {
    return schema.getDereferencedType() == DataSchema.Type.ARRAY;
  }

  private static void generateDataComplexConstructor(JDefinedClass cls,
      JClass dataComplexClass,
      JClass specificValueCollectionClass)
  {
    final JMethod constructor = cls.constructor(JMod.PUBLIC);
    constructor.body().invoke(THIS).arg(JExpr._new(dataComplexClass).arg(JExpr._new(specificValueCollectionClass)));
  }

  private static void generateConstructorWithObjectArg(JDefinedClass cls, JVar schemaField, JVar changeListenerVar, JVar specificMapVar)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar param = argConstructor.param(Object.class, "data");
    argConstructor.body().invoke(SUPER).arg(param).arg(schemaField);
    JBlock changeListenerBody = argConstructor.body();

    if (specificMapVar != null)
    {
      argConstructor.body().assign(specificMapVar,
          JOp.cond(JExpr.ref("_specificMap")._instanceof(specificMapVar.type()),
              JExpr.cast(specificMapVar.type(), JExpr.ref("_specificMap")), JExpr._null()));
      changeListenerBody = argConstructor.body()._if(specificMapVar.eq(JExpr._null()))._then();
    }

    if (changeListenerVar != null)
    {
      changeListenerBody.assign(changeListenerVar, JExpr._new(changeListenerVar.type()).arg(JExpr._this()));
      addChangeListenerRegistration(changeListenerBody, changeListenerVar);
    }
  }

  private static void generateConstructorWithArg(JDefinedClass cls, JVar schemaField, JClass paramClass, JVar changeListenerVar, JVar specificMapVar)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar param = argConstructor.param(paramClass, "data");
    argConstructor.body().invoke(SUPER).arg(param).arg(schemaField);
    JBlock changeListenerBody = argConstructor.body();

    if (specificMapVar != null)
    {
      argConstructor.body().assign(specificMapVar,
          JOp.cond(JExpr.ref("_specificMap")._instanceof(specificMapVar.type()),
              JExpr.cast(specificMapVar.type(), JExpr.ref("_specificMap")), JExpr._null()));
      changeListenerBody = argConstructor.body()._if(specificMapVar.eq(JExpr._null()))._then();
    }

    if (changeListenerVar != null)
    {
      changeListenerBody.assign(changeListenerVar, JExpr._new(changeListenerVar.type()).arg(JExpr._this()));
      addChangeListenerRegistration(changeListenerBody, changeListenerVar);
    }
  }

  private static void generateConstructorWithArg(JDefinedClass cls, JVar schemaField, JClass paramClass, JClass elementClass, JClass dataClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar param = argConstructor.param(paramClass, "data");
    final JInvocation inv = argConstructor.body().invoke(SUPER).arg(param).arg(schemaField).arg(JExpr.dotclass(elementClass));
    dataClassArg(inv, dataClass);
  }

  private static void addChangeListenerRegistration(JBlock body, JVar changeListenerVar)
  {
    body.invoke("addChangeListener").arg(changeListenerVar);
  }

  /**
   * Return the {@link DataSchema} for the array items or map values of the generated class.
   * <p/>
   * <p/>
   * When there is both an array of X and array of typeref XRef to X, both of these cases maps to the same generated class name, i.e. XArray. However, their schema is slightly different. From a
   * compile time binding perspective, the generated class is the same except the SCHEMA field may have different schema strings.
   * <p/>
   * <p/>
   * An option to retain the schema differences is to emit a different class for each different schema. The generator could emit XArray and XRefArray. However, this would lead to a proliferation of
   * generated classes for different array or maps of typerefs of the same type. This is not ideal as a common pattern is to use typeref to differentiate different uses of string.
   * <p/>
   * <p/>
   * To avoid a proliferation of classes and maintain backwards compatibility, the generator will always emit map and array whose values and items types are dereferenced to the native type or to the
   * first typeref with a custom Java class binding.
   *
   * @param customInfo provides the first {@link CustomInfoSpec} of an array's items or a map's values.
   * @param schema     provides the {@link DataSchema} of an array's items or a map's values.
   *
   * @return the {@link DataSchema} for the array items or map values of the generated class.
   */
  private static DataSchema schemaForArrayItemsOrMapValues(CustomInfoSpec customInfo, DataSchema schema)
  {
    return customInfo != null ? customInfo.getCustomSchema() : schema.getDereferencedDataSchema();
  }

  private void overrideCopierMethod(JDefinedClass templateClass, String methodName, Map<String, JVar> fields,
      Map<String, JVar> hasFields, boolean resetFields, JClass changeListenerClass, JVar specificMapVar)
  {
    final JMethod copierMethod = templateClass.method(JMod.PUBLIC, templateClass, methodName);
    copierMethod.annotate(Override.class);
    copierMethod._throws(CloneNotSupportedException.class);
    JVar copyVar = copierMethod.body().decl(templateClass, "__" + methodName, JExpr.cast(templateClass, JExpr._super().invoke(methodName)));

    if (!fields.isEmpty())
    {
      if (resetFields)
      {
        fields.forEach((key, var) -> {
          JVar hasVar =  hasFields.get(key);
          if (hasVar != null)
          {
            copierMethod.body().assign(copyVar.ref(hasVar), JExpr.lit(false));
            copierMethod.body().assign(copyVar.ref(var), getDefaultPrimitiveExpression(var.type()));
          }
          else
          {
            copierMethod.body().assign(copyVar.ref(var), JExpr._null());
          }
        });
      }

      JBlock isChangeListenerNotNull =
          copierMethod.body()._if(JExpr.ref("__changeListener").ne(JExpr._null()))._then();
      isChangeListenerNotNull.assign(copyVar.ref("__changeListener"), JExpr._new(changeListenerClass).arg(copyVar));
      isChangeListenerNotNull.add(copyVar.invoke("addChangeListener").arg(copyVar.ref("__changeListener")));
    }

    // Reset the specific map from the super class
    if (specificMapVar != null)
    {
      copierMethod.body().assign(copyVar.ref(specificMapVar.name()),
          JExpr.cast(specificMapVar.type(), copyVar.ref("_specificMap")));
    }

    copierMethod.body()._return(copyVar);
  }

  private static void setDeprecatedAnnotationAndJavadoc(DataSchema schema, JDefinedClass schemaClass)
  {
    setDeprecatedAnnotationAndJavadoc(schema.getProperties().get(DEPRECATED_KEY), schemaClass, schemaClass);
  }

  private static void setDeprecatedAnnotationAndJavadoc(JMethod method, RecordDataSchema.Field field)
  {
    setDeprecatedAnnotationAndJavadoc(field.getProperties().get(DEPRECATED_KEY), method, method);
  }

  private static void setDeprecatedAnnotationAndJavadoc(EnumDataSchema enumSchema, String symbol, JEnumConstant constant)
  {
    final Object deprecatedSymbolsProp = enumSchema.getProperties().get(DataSchemaConstants.DEPRECATED_SYMBOLS_KEY);
    if (deprecatedSymbolsProp instanceof DataMap)
    {
      final DataMap deprecatedSymbols = (DataMap) deprecatedSymbolsProp;

      final Object deprecatedProp = deprecatedSymbols.get(symbol);
      setDeprecatedAnnotationAndJavadoc(deprecatedProp, constant, constant);
    }
  }

  private static void setDeprecatedAnnotationAndJavadoc(Object deprecatedProp, JAnnotatable annotatable, JDocCommentable commentable)
  {
    if (Boolean.TRUE.equals(deprecatedProp) && annotatable != null)
    {
      annotatable.annotate(Deprecated.class);
    }
    else if (deprecatedProp instanceof String)
    {
      if (commentable != null)
      {
        final String deprecatedReason = (String) deprecatedProp;
        commentable.javadoc().addDeprecated().append(deprecatedReason);
      }
      if (annotatable != null)
      {
        annotatable.annotate(Deprecated.class);
      }
    }
  }

  private static int getJModValue(Set<ModifierSpec> modifiers)
  {
    try
    {
      int value = 0;
      for (ModifierSpec mod : modifiers)
      {
        value |= JMod.class.getDeclaredField(mod.name()).getInt(null);
      }
      return value;
    }
    catch (NoSuchFieldException e)
    {
      throw new RuntimeException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new RuntimeException(e);
    }
  }

  private static void addAccessorDoc(JClass clazz, JMethod method, RecordDataSchema.Field field, String prefix)
  {
    method.javadoc().append(prefix + " for " + field.getName());
    method.javadoc().addXdoclet("see " + clazz.name() + ".Fields#" + escapeReserved(field.getName()));
  }

  private JDefinedClass defineClass(ClassTemplateSpec classTemplateSpec)
      throws JClassAlreadyExistsException
  {
    JDefinedClass result = _definedClasses.get(classTemplateSpec);
    if (result == null)
    {
      final int jmodValue = getJModValue(classTemplateSpec.getModifiers());
      final JClassContainer container;
      if (classTemplateSpec.getEnclosingClass() == null)
      {
        container = getPackage(classTemplateSpec.getPackage());
      }
      else
      {
        container = defineClass(classTemplateSpec.getEnclosingClass());
      }

      if (classTemplateSpec instanceof ArrayTemplateSpec ||
          classTemplateSpec instanceof FixedTemplateSpec ||
          classTemplateSpec instanceof MapTemplateSpec ||
          classTemplateSpec instanceof RecordTemplateSpec ||
          classTemplateSpec instanceof TyperefTemplateSpec ||
          classTemplateSpec instanceof UnionTemplateSpec)
      {
        result = container._class(jmodValue, escapeReserved(classTemplateSpec.getClassName()));
      }
      else if (classTemplateSpec instanceof EnumTemplateSpec)
      {
        result = container._class(jmodValue, escapeReserved(classTemplateSpec.getClassName()), ClassType.ENUM);
      }
      else
      {
        throw new RuntimeException();
      }

      _definedClasses.put(classTemplateSpec, result);
    }

    return result;
  }

  protected void generateArray(JDefinedClass arrayClass, ArrayTemplateSpec arrayDataTemplateSpec)
      throws JClassAlreadyExistsException
  {
    final JClass itemJClass = generate(arrayDataTemplateSpec.getItemClass());
    final JClass dataJClass = getDataClass(arrayDataTemplateSpec.getSchema().getItems());
    final JClass specificElementArrayClass = getSpecificElementArrayClass(arrayDataTemplateSpec.getSchema().getItems());

    final boolean isDirect = CodeUtil.isDirectType(arrayDataTemplateSpec.getSchema().getItems());
    if (isDirect)
    {
      arrayClass._extends(_directArrayBaseClass.narrow(itemJClass));
    }
    else
    {
      extendWrappingArrayBaseClass(itemJClass, arrayClass);
    }

    /** see {@link #schemaForArrayItemsOrMapValues} */
    final DataSchema bareSchema = new ArrayDataSchema(schemaForArrayItemsOrMapValues(arrayDataTemplateSpec.getCustomInfo(), arrayDataTemplateSpec.getSchema().getItems()));
    final JVar schemaField = generateSchemaField(arrayClass, bareSchema, arrayDataTemplateSpec.getSourceFileFormat());
    final JClass specificArrayProviderClass = generateSpecificListProvider(arrayClass, specificElementArrayClass, itemJClass, dataJClass);
    arrayClass.field(JMod.PUBLIC | JMod.STATIC | JMod.FINAL, specificArrayProviderClass,
        "SPECIFIC_DATA_COMPLEX_PROVIDER", JExpr._new(specificArrayProviderClass));

    generateDataComplexConstructor(arrayClass, _dataListClass, specificElementArrayClass);
    generateConstructorWithInitialCapacity(arrayClass, _dataListClass, specificElementArrayClass);
    generateConstructorWithCollection(arrayClass, itemJClass, specificElementArrayClass);
    generateConstructorWithArg(arrayClass, schemaField, _dataListClass, itemJClass, dataJClass);
    generateConstructorWithVarArgs(arrayClass, itemJClass, specificElementArrayClass);

    if (_pathSpecMethods)
    {
      generatePathSpecMethodsForCollection(arrayClass, arrayDataTemplateSpec.getSchema(), itemJClass, "items");
    }

    generateCustomClassInitialization(arrayClass, arrayDataTemplateSpec.getCustomInfo());

    if (_copierMethods)
    {
      generateCopierMethods(arrayClass, Collections.emptyMap(), null, null, null);
    }

    // Generate coercer overrides
    generateCoercerOverrides(arrayClass,
        arrayDataTemplateSpec.getItemClass(),
        arrayDataTemplateSpec.getSchema().getItems(),
        arrayDataTemplateSpec.getCustomInfo(),
        false);
  }

  protected void extendWrappingArrayBaseClass(JClass itemJClass, JDefinedClass arrayClass)
  {
    arrayClass._extends(_wrappingArrayBaseClass.narrow(itemJClass));
  }

  protected void generateEnum(JDefinedClass enumClass, EnumTemplateSpec enumSpec)
  {
    enumClass.javadoc().append(enumSpec.getSchema().getDoc());

    setDeprecatedAnnotationAndJavadoc(enumSpec.getSchema(), enumClass);

    generateSchemaField(enumClass, enumSpec.getSchema(), enumSpec.getSourceFileFormat());

    for (String value : enumSpec.getSchema().getSymbols())
    {
      if (isReserved(value))
      {
        throw new IllegalArgumentException("Enum contains Java reserved symbol: " + value + " schema: " + enumSpec.getSchema());
      }

      final JEnumConstant enumConstant = enumClass.enumConstant(value);

      final String enumConstantDoc = enumSpec.getSchema().getSymbolDocs().get(value);

      if (enumConstantDoc != null)
      {
        enumConstant.javadoc().append(enumConstantDoc);
      }

      setDeprecatedAnnotationAndJavadoc(enumSpec.getSchema(), value, enumConstant);
    }
    enumClass.enumConstant(DataTemplateUtil.UNKNOWN_ENUM);

    JVar internedSymbol =
        enumClass.field(JMod.PRIVATE | JMod.FINAL, getCodeModel().ref(String.class), "__internedSymbol");

    JMethod constructor = enumClass.constructor(JMod.PRIVATE);
    constructor.body().assign(internedSymbol, JExpr.invoke("name").invoke("intern"));

    JMethod toString = enumClass.method(JMod.PUBLIC, getCodeModel().ref(String.class), "toString");
    toString.annotate(Override.class);
    toString.body()._return(internedSymbol);
  }

  protected void generateFixed(JDefinedClass fixedClass, FixedTemplateSpec fixedSpec)
  {
    fixedClass.javadoc().append(fixedSpec.getSchema().getDoc());

    setDeprecatedAnnotationAndJavadoc(fixedSpec.getSchema(), fixedClass);

    fixedClass._extends(FixedTemplate.class);

    final JVar schemaField = generateSchemaField(fixedClass, fixedSpec.getSchema(), fixedSpec.getSourceFileFormat());

    final JMethod bytesConstructor = fixedClass.constructor(JMod.PUBLIC);
    final JVar param = bytesConstructor.param(ByteString.class, "value");
    bytesConstructor.body().invoke(SUPER).arg(param).arg(schemaField);

    generateConstructorWithObjectArg(fixedClass, schemaField, null, null);

    if (_copierMethods)
    {
      generateCopierMethods(fixedClass, Collections.emptyMap(), null, null,null);
    }
  }

  protected void generateMap(JDefinedClass mapClass, MapTemplateSpec mapSpec)
      throws JClassAlreadyExistsException
  {
    final JClass valueJClass = generate(mapSpec.getValueClass());
    final JClass dataJClass = getDataClass(mapSpec.getSchema().getValues());
    final JClass specificValueMapClass = getSpecificValueMapClass(mapSpec.getSchema().getValues());

    final boolean isDirect = CodeUtil.isDirectType(mapSpec.getSchema().getValues());
    if (isDirect)
    {
      mapClass._extends(_directMapBaseClass.narrow(valueJClass));
    }
    else
    {
      extendWrappingMapBaseClass(valueJClass, mapClass);
    }

    final DataSchema bareSchema = new MapDataSchema(schemaForArrayItemsOrMapValues(mapSpec.getCustomInfo(), mapSpec.getSchema().getValues()));
    final JVar schemaField = generateSchemaField(mapClass, bareSchema, mapSpec.getSourceFileFormat());
    final JClass specificMapProviderClass = generateSpecificMapProvider(mapClass, specificValueMapClass, valueJClass, dataJClass);
    mapClass.field(JMod.PUBLIC | JMod.STATIC | JMod.FINAL, specificMapProviderClass,
        "SPECIFIC_DATA_COMPLEX_PROVIDER", JExpr._new(specificMapProviderClass));

    generateDataComplexConstructor(mapClass, _dataMapClass, specificValueMapClass);
    generateConstructorWithInitialCapacity(mapClass, _dataMapClass, specificValueMapClass);
    generateConstructorWithInitialCapacityAndLoadFactor(mapClass, specificValueMapClass);
    generateConstructorWithMap(mapClass, valueJClass);
    generateConstructorWithArg(mapClass, schemaField, _dataMapClass, valueJClass, dataJClass);

    if (_pathSpecMethods)
    {
      generatePathSpecMethodsForCollection(mapClass, mapSpec.getSchema(), valueJClass, "values");
    }

    generateCustomClassInitialization(mapClass, mapSpec.getCustomInfo());

    if (_copierMethods)
    {
      generateCopierMethods(mapClass, Collections.emptyMap(), null, null, null);
    }

    // Generate coercer overrides
    generateCoercerOverrides(mapClass,
        mapSpec.getValueClass(),
        mapSpec.getSchema().getValues(),
        mapSpec.getCustomInfo(),
        true);
  }

  protected void extendWrappingMapBaseClass(JClass valueJClass, JDefinedClass mapClass)
  {
    mapClass._extends(_wrappingMapBaseClass.narrow(valueJClass));
  }

  private JClass generatePrimitive(PrimitiveTemplateSpec primitiveSpec)
  {
    switch (primitiveSpec.getSchema().getType())
    {
      case INT:
        return getCodeModel().INT.boxify();

      case DOUBLE:
        return getCodeModel().DOUBLE.boxify();

      case BOOLEAN:
        return getCodeModel().BOOLEAN.boxify();

      case STRING:
        return _stringClass;

      case LONG:
        return getCodeModel().LONG.boxify();

      case FLOAT:
        return getCodeModel().FLOAT.boxify();

      case BYTES:
        return _byteStringClass;

      default:
        throw new RuntimeException("Not supported primitive: " + primitiveSpec);
    }
  }

  private JClass getDataClass(DataSchema schema)
  {
    switch (schema.getDereferencedType())
    {
      case INT:
        return getCodeModel().INT.boxify();
      case FLOAT:
        return getCodeModel().FLOAT.boxify();
      case LONG:
        return getCodeModel().LONG.boxify();
      case DOUBLE:
        return getCodeModel().DOUBLE.boxify();
      case FIXED:
      case BYTES:
        return _byteStringClass;
      case BOOLEAN:
        return getCodeModel().BOOLEAN.boxify();
      case STRING:
      case ENUM:
        return _stringClass;
      case MAP:
      case RECORD:
      case UNION:
        return _dataMapClass;
      case ARRAY:
        return _dataListClass;
      default:
        throw new TemplateOutputCastException(
            "Cannot handle wrapped schema of type " + schema.getDereferencedType());
    }
  }

  private JClass getSpecificValueMapClass(DataSchema schema)
  {
    switch (schema.getDereferencedType())
    {
      case INT:
        return getCodeModel().ref(IntegerMap.IntegerSpecificValueMap.class);
      case FLOAT:
        return getCodeModel().ref(FloatMap.FloatSpecificValueMap.class);
      case LONG:
        return getCodeModel().ref(LongMap.LongSpecificValueMap.class);
      case DOUBLE:
        return getCodeModel().ref(DoubleMap.DoubleSpecificValueMap.class);
      case FIXED:
      case BYTES:
        return getCodeModel().ref(BytesMap.ByteStringSpecificValueMap.class);
      case BOOLEAN:
        return getCodeModel().ref(BooleanMap.BooleanSpecificValueMap.class);
      case STRING:
      case ENUM:
        return getCodeModel().ref(StringMap.StringSpecificValueMap.class);
      case MAP:
      case RECORD:
      case UNION:
        return getCodeModel().ref(WrappingMapTemplate.DataMapSpecificValueMap.class);
      case ARRAY:
        return getCodeModel().ref(WrappingMapTemplate.DataListSpecificValueMap.class);
      default:
        throw new TemplateOutputCastException(
            "Cannot handle wrapped schema of type " + schema.getDereferencedType());
    }
  }

  private JClass getSpecificElementArrayClass(DataSchema schema)
  {
    switch (schema.getDereferencedType())
    {
      case INT:
        return getCodeModel().ref(IntegerArray.IntegerSpecificElementArray.class);
      case FLOAT:
        return getCodeModel().ref(FloatArray.FloatSpecificElementArray.class);
      case LONG:
        return getCodeModel().ref(LongArray.LongSpecificElementArray.class);
      case DOUBLE:
        return getCodeModel().ref(DoubleArray.DoubleSpecificElementArray.class);
      case FIXED:
      case BYTES:
        return getCodeModel().ref(BytesArray.ByteStringSpecificElementArray.class);
      case BOOLEAN:
        return getCodeModel().ref(BooleanArray.BooleanSpecificElementArray.class);
      case STRING:
      case ENUM:
        return getCodeModel().ref(StringArray.StringSpecificElementArray.class);
      case MAP:
      case RECORD:
      case UNION:
        return getCodeModel().ref(WrappingArrayTemplate.DataMapSpecificElementArray.class);
      case ARRAY:
        return getCodeModel().ref(WrappingArrayTemplate.DataListSpecificElementArray.class);
      default:
        throw new TemplateOutputCastException(
            "Cannot handle wrapped schema of type " + schema.getDereferencedType());
    }
  }

  protected void generateRecord(JDefinedClass templateClass, RecordTemplateSpec recordSpec)
      throws JClassAlreadyExistsException
  {
    templateClass.javadoc().append(recordSpec.getSchema().getDoc());

    setDeprecatedAnnotationAndJavadoc(recordSpec.getSchema(), templateClass);

    extendRecordBaseClass(templateClass);

    if (_pathSpecMethods)
    {
      generatePathSpecMethodsForRecord(recordSpec.getFields(), templateClass);
    }

    final JFieldVar schemaFieldVar = generateSchemaField(templateClass, recordSpec.getSchema(), recordSpec.getSourceFileFormat());

    // Generate instance vars
    Map<String, JVar> fieldVarMap = new HashMap<>();
    Map<String, JVar> hasFieldVarMap = new HashMap<>();
    Map<String, JType> fieldDataClassMap = new HashMap<>();
    Map<String, JExpression> fieldNameExprMap = new HashMap<>();
    for (RecordTemplateSpec.Field field : recordSpec.getFields())
    {
      final String fieldName = field.getSchemaField().getName();
      JType fieldType = generate(field.getType());
      JType unboxedFieldType = fieldType.unboxify();

      final JVar fieldVar;
      if (fieldType.equals(unboxedFieldType))
      {
        fieldVar = templateClass.field(JMod.PRIVATE, fieldType, "_" + fieldName + "Field", JExpr._null());
      }
      else
      {
        fieldVar = templateClass.field(JMod.PRIVATE, unboxedFieldType, "_" + fieldName + "Field",
            getDefaultPrimitiveExpression(unboxedFieldType));
        JVar hasFieldVar = templateClass.field(JMod.PRIVATE, getCodeModel().BOOLEAN,
            "_has" + StringUtils.capitalize(fieldName) + "Field", JExpr.lit(false));
        hasFieldVarMap.put(fieldName, hasFieldVar);
      }

      fieldVarMap.put(fieldName, fieldVar);
      JType type = getDataClass(field.getSchemaField().getType());
      fieldDataClassMap.put(fieldName, type);

      final JExpression fieldNameExpr = JExpr.ref("FIELD_" + StringUtils.capitalize(fieldName)).invoke("getName");
      fieldNameExprMap.put(fieldName, fieldNameExpr);
    }

    final JVar changeListenerVar;
    final JClass changeListenerClass;
    // Generate a change listener if there are any fields.
    if (!fieldVarMap.isEmpty())
    {
      changeListenerClass = generateChangeListener(templateClass, fieldVarMap, hasFieldVarMap);
      changeListenerVar = templateClass.field(JMod.PRIVATE, changeListenerClass, "__changeListener");
    }
    else
    {
      changeListenerClass = null;
      changeListenerVar = null;
    }

    final JClass specificMapClass = generateSpecificMap(templateClass, fieldVarMap, fieldDataClassMap, fieldNameExprMap);
    final JVar specificMapVar = templateClass.field(JMod.PRIVATE, specificMapClass, "__specificMap");
    final JClass specificMapProviderClass =
        generateSpecificMapProvider(templateClass, fieldVarMap, fieldDataClassMap, specificMapClass);
    templateClass.field(JMod.PUBLIC | JMod.STATIC | JMod.FINAL, specificMapProviderClass,
        "SPECIFIC_DATA_COMPLEX_PROVIDER", JExpr._new(specificMapProviderClass));

    generateDataMapConstructor(templateClass, schemaFieldVar, specificMapVar);
    generateConstructorWithArg(templateClass, schemaFieldVar, _dataMapClass, changeListenerVar, specificMapVar);

    recordSpec.getFields().stream()
        .map(RecordTemplateSpec.Field::getCustomInfo)
        .distinct()
        .forEach(customInfo -> generateCustomClassInitialization(templateClass, customInfo));

    // Generate accessors
    for (RecordTemplateSpec.Field field : recordSpec.getFields())
    {
      final String fieldName = field.getSchemaField().getName();
      generateRecordFieldAccessors(templateClass, field, generate(field.getType()), schemaFieldVar,
          fieldVarMap.get(fieldName), hasFieldVarMap.get(fieldName), specificMapVar);
    }

    if (_copierMethods)
    {
      generateCopierMethods(templateClass, fieldVarMap, hasFieldVarMap, changeListenerClass, specificMapVar);
    }
  }

  /**
   * Generates a constructor with no arguments for a DataTemplate type. The constructor calls the super class
   * constructor that accepts a new instance of "DataMap" type (provided by _dataMapClass) and the SCHEMA.
   * @param cls DataTemplate class being constructed.
   * @param schemaField SCHEMA field to use for initialization.
   * @param specificMapVar The specific map variable.
   */
  private void generateDataMapConstructor(JDefinedClass cls, JVar schemaField, JVar specificMapVar)
  {
    final JMethod noArgConstructor = cls.constructor(JMod.PUBLIC);
    noArgConstructor.body().invoke(SUPER).arg(JExpr._new(specificMapVar.type())).arg(schemaField);
    noArgConstructor.body().assign(specificMapVar, JExpr.cast(specificMapVar.type(), JExpr._this().ref("_specificMap")));
  }

  protected void extendRecordBaseClass(JDefinedClass templateClass)
  {
    templateClass._extends(_recordBaseClass);
  }

  private void generatePathSpecMethodsForRecord(List<RecordTemplateSpec.Field> fieldSpecs, JDefinedClass templateClass)
      throws JClassAlreadyExistsException
  {
    final JDefinedClass fieldsNestedClass = generatePathSpecNestedClass(templateClass);

    for (RecordTemplateSpec.Field field : fieldSpecs)
    {
      JClass fieldsRefType = _pathSpecClass;
      if (hasNestedFields(field.getSchemaField().getType()))
      {
        final JClass fieldType = generate(field.getType());
        fieldsRefType = getCodeModel().ref(fieldType.fullName() + ".Fields");
      }

      final JMethod constantField = fieldsNestedClass.method(JMod.PUBLIC, fieldsRefType, escapeReserved(field.getSchemaField().getName()));
      constantField.body()._return(JExpr._new(fieldsRefType).arg(JExpr.invoke("getPathComponents")).arg(field.getSchemaField().getName()));
      if (!field.getSchemaField().getDoc().isEmpty())
      {
        constantField.javadoc().append(field.getSchemaField().getDoc());
      }
      setDeprecatedAnnotationAndJavadoc(constantField, field.getSchemaField());

      // For array types, add another method to get PathSpec with a range specified
      if (isArrayType(field.getSchemaField().getType()))
      {
        final JMethod pathSpecRangeMethod = fieldsNestedClass.method(JMod.PUBLIC, _pathSpecClass, escapeReserved(field.getSchemaField().getName()));
        final JVar arrayPathSpec = pathSpecRangeMethod.body()
            .decl(_pathSpecClass, "arrayPathSpec",
                JExpr._new(_pathSpecClass).arg(JExpr.invoke("getPathComponents")).arg(field.getSchemaField().getName()));
        JClass integerClass = generate(PrimitiveTemplateSpec.getInstance(DataSchema.Type.INT));
        JVar start = pathSpecRangeMethod.param(integerClass, "start");
        pathSpecRangeMethod.body()._if(start.ne(JExpr._null())).
            _then().invoke(arrayPathSpec, "setAttribute").arg(PathSpec.ATTR_ARRAY_START).arg(start);
        JVar count = pathSpecRangeMethod.param(integerClass, "count");
        pathSpecRangeMethod.body()._if(count.ne(JExpr._null()))
            ._then().invoke(arrayPathSpec, "setAttribute").arg(PathSpec.ATTR_ARRAY_COUNT).arg(count);
        pathSpecRangeMethod.body()._return(arrayPathSpec);

        if (!field.getSchemaField().getDoc().isEmpty())
        {
          pathSpecRangeMethod.javadoc().append(field.getSchemaField().getDoc());
        }
        setDeprecatedAnnotationAndJavadoc(pathSpecRangeMethod, field.getSchemaField());
      }
    }

    final JVar staticFields = templateClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, fieldsNestedClass, "_fields").init(JExpr._new(fieldsNestedClass));
    final JMethod staticFieldsAccessor = templateClass.method(JMod.PUBLIC | JMod.STATIC, fieldsNestedClass, "fields");
    staticFieldsAccessor.body()._return(staticFields);
  }

  private void generateRecordFieldAccessors(JDefinedClass templateClass, RecordTemplateSpec.Field field, JClass type, JVar schemaFieldVar,
      JVar fieldVar, JVar hasFieldVar, JVar specificMapVar)
  {
    final RecordDataSchema.Field schemaField = field.getSchemaField();
    final DataSchema fieldSchema = schemaField.getType();
    final String capitalizedName = CodeUtil.capitalize(schemaField.getName());
    final String rawFieldVarName = fieldVar.name() + "Raw";
    final String primitiveRawFieldVarName = fieldVar.name() + "PrimitiveRaw";
    final String isSchemaTypeVarName = "_isSchemaType" +  StringUtils.capitalize(fieldVar.name());
    final String coercedFieldVarName = fieldVar.name() + "Coerced";
    final String hasCoercedFieldVarName = "_has" + StringUtils.capitalize(fieldVar.name()) + "Coerced";

    final JExpression mapRef = JExpr._super().ref("_map");
    final String fieldFieldName = "FIELD_" + capitalizedName;
    final JExpression fieldNameExpr = JExpr.ref(fieldFieldName).invoke("getName");
    final JFieldVar fieldField = templateClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, RecordDataSchema.Field.class, fieldFieldName);
    fieldField.init(schemaFieldVar.invoke("getField").arg(schemaField.getName()));

    // Generate default field if applicable
    final String defaultFieldName = "DEFAULT_" + capitalizedName;
    final JFieldVar defaultField;
    if (field.getSchemaField().getDefault() != null)
    {
      defaultField = templateClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, type, defaultFieldName);

      templateClass.init().assign(defaultField, getCoerceOutputExpression(
          fieldField.invoke("getDefault"), schemaField.getType(), type, field.getCustomInfo(), false));
    }
    else
    {
      defaultField = null;
    }

    // Generate has method.
    final JMethod has = templateClass.method(JMod.PUBLIC, getCodeModel().BOOLEAN, "has" + capitalizedName);
    addAccessorDoc(templateClass, has, schemaField, "Existence checker");
    setDeprecatedAnnotationAndJavadoc(has, schemaField);
    final JBlock hasBody = has.body();
    JVar localSpecificMapVar = hasBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    if (hasFieldVar != null)
    {
      hasBody._if(localSpecificMapVar.ne(JExpr._null()))._then()._return(
          localSpecificMapVar.ref(isSchemaTypeVarName).cor(localSpecificMapVar.ref(rawFieldVarName).ne(JExpr._null())));
      hasBody._if(hasFieldVar)._then()._return(JExpr.lit(true));
    }
    else
    {
      hasBody._if(localSpecificMapVar.ne(JExpr._null()))._then()._return(localSpecificMapVar.ref(rawFieldVarName).ne(JExpr._null()));
      hasBody._if(fieldVar.ne(JExpr._null()))._then()._return(JExpr.lit(true));
    }

    hasBody._return(mapRef.invoke("containsKey").arg(fieldNameExpr));

    if (_recordFieldRemove)
    {
      // Generate remove method.
      final String removeName = "remove" + capitalizedName;
      final JMethod remove = templateClass.method(JMod.PUBLIC, getCodeModel().VOID, removeName);
      addAccessorDoc(templateClass, remove, schemaField, "Remover");
      setDeprecatedAnnotationAndJavadoc(remove, schemaField);
      final JBlock removeBody = remove.body();
      localSpecificMapVar = removeBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
      JConditional specificMapRemoveCondition = removeBody._if(localSpecificMapVar.ne(JExpr._null()));
      JBlock notNullCondition;
      if (hasFieldVar != null)
      {
        notNullCondition = specificMapRemoveCondition._then()._if(
            localSpecificMapVar.ref(isSchemaTypeVarName).cor(localSpecificMapVar.ref(rawFieldVarName).ne(JExpr._null())))._then();
      }
      else
      {
        notNullCondition = specificMapRemoveCondition._then()._if(localSpecificMapVar.ref(rawFieldVarName).ne(JExpr._null()))._then();
      }
      notNullCondition.assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
      if (hasFieldVar != null)
      {
        notNullCondition.assign(localSpecificMapVar.ref(primitiveRawFieldVarName), getDefaultPrimitiveExpression(fieldVar.type()));
        notNullCondition.assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(false));
        notNullCondition.assign(localSpecificMapVar.ref(coercedFieldVarName), getDefaultPrimitiveExpression(fieldVar.type()));
        notNullCondition.assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(false));
      }
      else
      {
        notNullCondition.assign(localSpecificMapVar.ref(coercedFieldVarName), JExpr._null());
      }

      notNullCondition.assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(-1));
      specificMapRemoveCondition._else().add(mapRef.invoke("remove").arg(fieldNameExpr));
    }

    final String getterName = JavaCodeUtil.getGetterName(getCodeModel(), type, capitalizedName);

    if (_recordFieldAccessorWithMode)
    {
      // Getter method with mode.
      final JMethod getterWithMode = templateClass.method(JMod.PUBLIC, type, getterName);
      addAccessorDoc(templateClass, getterWithMode, schemaField, "Getter");
      setDeprecatedAnnotationAndJavadoc(getterWithMode, schemaField);
      JVar modeParam = getterWithMode.param(_getModeClass, "mode");
      final JBlock getterWithModeBody = getterWithMode.body();

      // If it is an optional field with no default, just call out to the getter without mode.
      if (field.getSchemaField().getOptional() && defaultField == null)
      {
        getterWithModeBody._return(JExpr.invoke(getterName));
      }
      else
      {
        JSwitch modeSwitch = getterWithModeBody._switch(modeParam);
        JCase strictCase = modeSwitch._case(JExpr.ref("STRICT"));
        // If there is no default defined, call the getter without mode, else fall through to default.
        if (defaultField == null)
        {
          strictCase.body()._return(JExpr.invoke(getterName));
        }
        JCase defaultCase = modeSwitch._case(JExpr.ref("DEFAULT"));
        if (defaultField != null)
        {
          // If there is a default, then default is the same as strict, else we should fall through to null.
          defaultCase.body()._return(JExpr.invoke(getterName));
        }

        JCase nullCase = modeSwitch._case(JExpr.ref("NULL"));
        localSpecificMapVar = nullCase.body().decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
        JBlock specificMapNotNull = nullCase.body()._if(localSpecificMapVar.ne(JExpr._null()))._then();
        JExpression rawValueInitExpr;

        if (hasFieldVar != null)
        {
          specificMapNotNull._if(localSpecificMapVar.ref(hasCoercedFieldVarName))._then()._return(localSpecificMapVar.ref(coercedFieldVarName));
          rawValueInitExpr = JOp.cond(localSpecificMapVar.ref(isSchemaTypeVarName),
              localSpecificMapVar.ref(primitiveRawFieldVarName), localSpecificMapVar.ref(rawFieldVarName));
        }
        else
        {
          specificMapNotNull._if(localSpecificMapVar.ref(coercedFieldVarName).ne(JExpr._null()))._then()._return(localSpecificMapVar.ref(coercedFieldVarName));
          rawValueInitExpr = localSpecificMapVar.ref(rawFieldVarName);
        }

        JVar rawValueVar = specificMapNotNull.decl(JMod.FINAL, _objectClass, "__rawValue", rawValueInitExpr);
        specificMapNotNull._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
        specificMapNotNull.assign(localSpecificMapVar.ref(coercedFieldVarName), getCoerceOutputExpression(rawValueVar, fieldSchema, type, field.getCustomInfo(), true));
        if (hasFieldVar != null)
        {
          specificMapNotNull.assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
        }
        specificMapNotNull._return(localSpecificMapVar.ref(coercedFieldVarName));

        if (hasFieldVar != null)
        {
          nullCase.body()._if(hasFieldVar)._then()._return(fieldVar);
        }
        else
        {
          nullCase.body()._if(fieldVar.ne(JExpr._null()))._then()._return(fieldVar);
        }

        rawValueVar = nullCase.body().decl(JMod.FINAL, _objectClass, "__rawValue", mapRef.invoke("get").arg(fieldNameExpr));
        nullCase.body()._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
        nullCase.body().assign(fieldVar, getCoerceOutputExpression(rawValueVar, fieldSchema, type, field.getCustomInfo(), true));
        nullCase.body()._return(fieldVar);

        getterWithModeBody._throw(JExpr._new(getCodeModel().ref(IllegalStateException.class)).arg(JExpr.lit("Unknown mode ").plus(modeParam)));
      }
    }

    // Getter method without mode.
    final JMethod getterWithoutMode = templateClass.method(JMod.PUBLIC, type, getterName);
    addAccessorDoc(templateClass, getterWithoutMode, schemaField, "Getter");
    setDeprecatedAnnotationAndJavadoc(getterWithoutMode, schemaField);
    JCommentPart returnComment = getterWithoutMode.javadoc().addReturn();
    if (schemaField.getOptional())
    {
      getterWithoutMode.annotate(Nullable.class);
      returnComment.add("Optional field. Always check for null.");
    }
    else
    {
      getterWithoutMode.annotate(Nonnull.class);
      returnComment.add("Required field. Could be null for partial record.");
    }
    final JBlock getterWithoutModeBody = getterWithoutMode.body();

    localSpecificMapVar = getterWithoutModeBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    JBlock specificMapNotNull = getterWithoutModeBody._if(localSpecificMapVar.ne(JExpr._null()))._then();
    JExpression rawValueInitExpr;
    if (hasFieldVar != null)
    {
      specificMapNotNull._if(localSpecificMapVar.ref(hasCoercedFieldVarName))._then()._return(localSpecificMapVar.ref(coercedFieldVarName));
      rawValueInitExpr = JOp.cond(localSpecificMapVar.ref(isSchemaTypeVarName),
          localSpecificMapVar.ref(primitiveRawFieldVarName), localSpecificMapVar.ref(rawFieldVarName));
    }
    else
    {
      specificMapNotNull._if(specificMapVar.ref(coercedFieldVarName).ne(JExpr._null()))._then()._return(localSpecificMapVar.ref(coercedFieldVarName));
      rawValueInitExpr = localSpecificMapVar.ref(rawFieldVarName);
    }

    JVar rawValueVar = specificMapNotNull.decl(JMod.FINAL, _objectClass, "__rawValue", rawValueInitExpr);
    if (schemaField.getDefault() != null)
    {
      specificMapNotNull._if(rawValueVar.eq(JExpr._null()))._then()._return(defaultField);
    }
    else if (!schemaField.getOptional())
    {
      specificMapNotNull._if(rawValueVar.eq(JExpr._null()))._then()._throw(
          JExpr._new(getCodeModel().ref(RequiredFieldNotPresentException.class)).arg(fieldNameExpr));
    }
    else
    {
      specificMapNotNull._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
    }

    specificMapNotNull.assign(localSpecificMapVar.ref(coercedFieldVarName), getCoerceOutputExpression(rawValueVar, fieldSchema, type, field.getCustomInfo(), true));
    if (hasFieldVar != null)
    {
      specificMapNotNull.assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
    }
    specificMapNotNull._return(localSpecificMapVar.ref(coercedFieldVarName));

    if (hasFieldVar != null)
    {
      getterWithoutModeBody._if(hasFieldVar)._then()._return(fieldVar);
    }
    else
    {
      getterWithoutModeBody._if(fieldVar.ne(JExpr._null()))._then()._return(fieldVar);
    }

    rawValueVar = getterWithoutModeBody.decl(
        JMod.FINAL, _objectClass, "__rawValue", mapRef.invoke("get").arg(fieldNameExpr));
    if (schemaField.getDefault() != null)
    {
      getterWithoutModeBody._if(rawValueVar.eq(JExpr._null()))._then()._return(defaultField);
    }
    else if (!schemaField.getOptional())
    {
      getterWithoutModeBody._if(rawValueVar.eq(JExpr._null()))._then()._throw(
          JExpr._new(getCodeModel().ref(RequiredFieldNotPresentException.class)).arg(fieldNameExpr));
    }
    else
    {
      getterWithoutModeBody._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
    }

    getterWithoutModeBody.assign(fieldVar,
        getCoerceOutputExpression(rawValueVar, fieldSchema, type, field.getCustomInfo(), true));
    getterWithoutModeBody._return(fieldVar);

    final String setterName = "set" + capitalizedName;

    if (_recordFieldAccessorWithMode)
    {
      // Setter method with mode
      final JMethod setterWithMode = templateClass.method(JMod.PUBLIC, templateClass, setterName);
      addAccessorDoc(templateClass, setterWithMode, schemaField, "Setter");
      setDeprecatedAnnotationAndJavadoc(setterWithMode, schemaField);
      JVar param = setterWithMode.param(type, "value");
      JVar modeParam = setterWithMode.param(_setModeClass, "mode");
      JSwitch modeSwitch = setterWithMode.body()._switch(modeParam);
      JCase disallowNullCase = modeSwitch._case(JExpr.ref("DISALLOW_NULL"));
      disallowNullCase.body()._return(JExpr.invoke(setterName).arg(param));

      // Generate remove optional if null, only for required fields. Optional fields will fall through to
      // remove if null, which is the same behavior for them.
      JCase removeOptionalIfNullCase = modeSwitch._case(JExpr.ref("REMOVE_OPTIONAL_IF_NULL"));
      if (!schemaField.getOptional()) {
        JConditional paramIsNull = removeOptionalIfNullCase.body()._if(param.eq(JExpr._null()));
        paramIsNull._then()._throw(JExpr._new(getCodeModel().ref(IllegalArgumentException.class))
            .arg(JExpr.lit("Cannot remove mandatory field " + schemaField.getName() + " of " + templateClass.fullName())));
        JBlock elseBlock = paramIsNull._else();
        rawValueVar = elseBlock.decl(JMod.FINAL, _objectClass, "__rawValue", getCoerceInputExpression(param, fieldSchema, field.getCustomInfo()));
        localSpecificMapVar = elseBlock.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
        JConditional specificMapSetCondition = elseBlock._if(localSpecificMapVar.ne(JExpr._null()));
        JExpression specificMapSizeCondition;
        if (hasFieldVar != null)
        {
          specificMapSizeCondition = localSpecificMapVar.ref(isSchemaTypeVarName).not().cand(localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null()));
        }
        else
        {
          specificMapSizeCondition = localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null());
        }

        specificMapSetCondition._then()._if(specificMapSizeCondition)._then().assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(1));
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(coercedFieldVarName), param);
        if (hasFieldVar != null)
        {
          specificMapSetCondition._then().assign(localSpecificMapVar.ref(primitiveRawFieldVarName), JExpr.cast(fieldVar.type(), rawValueVar));
          specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
          specificMapSetCondition._then().assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
        }
        else
        {
          specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), rawValueVar);
        }

        specificMapSetCondition._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
        specificMapSetCondition._else().add(
            _checkedUtilClass.staticInvoke("putWithoutChecking")
                .arg(mapRef).arg(fieldNameExpr).arg(rawValueVar));
        specificMapSetCondition._else().assign(fieldVar, param);
        if (hasFieldVar != null)
        {
          specificMapSetCondition._else().assign(hasFieldVar, JExpr.lit(true));
        }
        removeOptionalIfNullCase.body()._break();
      }

      JCase removeIfNullCase = modeSwitch._case(JExpr.ref("REMOVE_IF_NULL"));
      JConditional paramIsNull = removeIfNullCase.body()._if(param.eq(JExpr._null()));
      paramIsNull._then().invoke("remove" + capitalizedName);
      JBlock elseBlock = paramIsNull._else();
      rawValueVar = elseBlock.decl(JMod.FINAL, _objectClass, "__rawValue", getCoerceInputExpression(param, fieldSchema, field.getCustomInfo()));
      localSpecificMapVar = elseBlock.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
      JConditional specificMapSetCondition = elseBlock._if(localSpecificMapVar.ne(JExpr._null()));
      JExpression specificMapSizeCondition;
      if (hasFieldVar != null)
      {
        specificMapSizeCondition = localSpecificMapVar.ref(isSchemaTypeVarName).not().cand(localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null()));
      }
      else
      {
        specificMapSizeCondition = localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null());
      }

      specificMapSetCondition._then()._if(specificMapSizeCondition)._then().assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(1));
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(coercedFieldVarName), param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(primitiveRawFieldVarName), JExpr.cast(fieldVar.type(), rawValueVar));
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
      }
      else
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), rawValueVar);
      }

      specificMapSetCondition._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
      specificMapSetCondition._else().add(
          _checkedUtilClass.staticInvoke("putWithoutChecking")
              .arg(mapRef).arg(fieldNameExpr).arg(rawValueVar));
      specificMapSetCondition._else().assign(fieldVar, param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._else().assign(hasFieldVar, JExpr.lit(true));
      }
      removeIfNullCase.body()._break();

      JCase ignoreNullCase = modeSwitch._case(JExpr.ref("IGNORE_NULL"));
      JBlock paramIsNotNull = ignoreNullCase.body()._if(param.ne(JExpr._null()))._then();
      rawValueVar = paramIsNotNull.decl(JMod.FINAL, _objectClass, "__rawValue", getCoerceInputExpression(param, fieldSchema, field.getCustomInfo()));
      localSpecificMapVar = paramIsNotNull.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
      specificMapSetCondition = paramIsNotNull._if(localSpecificMapVar.ne(JExpr._null()));
      if (hasFieldVar != null)
      {
        specificMapSizeCondition = localSpecificMapVar.ref(isSchemaTypeVarName).not().cand(localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null()));
      }
      else
      {
        specificMapSizeCondition = localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null());
      }

      specificMapSetCondition._then()._if(specificMapSizeCondition)._then().assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(1));
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(coercedFieldVarName), param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(primitiveRawFieldVarName), JExpr.cast(fieldVar.type(), rawValueVar));
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
      }
      else
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), rawValueVar);
      }

      specificMapSetCondition._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
      specificMapSetCondition._else().add(
          _checkedUtilClass.staticInvoke("putWithoutChecking")
              .arg(mapRef).arg(fieldNameExpr).arg(rawValueVar));
      specificMapSetCondition._else().assign(fieldVar, param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._else().assign(hasFieldVar, JExpr.lit(true));
      }
      ignoreNullCase.body()._break();

      setterWithMode.body()._return(JExpr._this());
    }

    // Setter method without mode
    final JMethod setter = templateClass.method(JMod.PUBLIC, templateClass, setterName);
    addAccessorDoc(templateClass, setter, schemaField, "Setter");
    setDeprecatedAnnotationAndJavadoc(setter, schemaField);
    JVar param = setter.param(type, "value");
    param.annotate(Nonnull.class);
    JCommentPart paramDoc = setter.javadoc().addParam(param);
    paramDoc.add("Must not be null. For more control, use setters with mode instead.");
    JConditional paramIsNull = setter.body()._if(param.eq(JExpr._null()));
    paramIsNull._then()._throw(JExpr._new(getCodeModel().ref(NullPointerException.class))
        .arg(JExpr.lit("Cannot set field " + schemaField.getName() + " of " + templateClass.fullName() + " to null")));
    JBlock elseBlock = paramIsNull._else();
    rawValueVar = elseBlock.decl(JMod.FINAL, _objectClass, "__rawValue", getCoerceInputExpression(param, fieldSchema, field.getCustomInfo()));
    localSpecificMapVar = elseBlock.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    JConditional specificMapSetCondition = elseBlock._if(localSpecificMapVar.ne(JExpr._null()));
    JExpression specificMapSizeCondition;
    if (hasFieldVar != null)
    {
      specificMapSizeCondition = localSpecificMapVar.ref(isSchemaTypeVarName).not().cand(localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null()));
    }
    else
    {
      specificMapSizeCondition = localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null());
    }

    specificMapSetCondition._then()._if(specificMapSizeCondition)._then().assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(1));
    specificMapSetCondition._then().assign(localSpecificMapVar.ref(coercedFieldVarName), param);
    if (hasFieldVar != null)
    {
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(primitiveRawFieldVarName), JExpr.cast(fieldVar.type(), rawValueVar));
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
    }
    else
    {
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), rawValueVar);
    }

    specificMapSetCondition._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
    specificMapSetCondition._else().add(
        _checkedUtilClass.staticInvoke("putWithoutChecking")
            .arg(mapRef).arg(fieldNameExpr).arg(rawValueVar));
    specificMapSetCondition._else().assign(fieldVar, param);
    if (hasFieldVar != null)
    {
      specificMapSetCondition._else().assign(hasFieldVar, JExpr.lit(true));
    }
    setter.body()._return(JExpr._this());

    // Setter method without mode for unboxified type
    if (!type.unboxify().equals(type))
    {
      final JMethod unboxifySetter = templateClass.method(JMod.PUBLIC, templateClass, setterName);
      addAccessorDoc(templateClass, unboxifySetter, schemaField, "Setter");
      setDeprecatedAnnotationAndJavadoc(unboxifySetter, schemaField);
      param = unboxifySetter.param(type.unboxify(), "value");
      localSpecificMapVar = unboxifySetter.body().decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
      specificMapSetCondition = unboxifySetter.body()._if(localSpecificMapVar.ne(JExpr._null()));
      if (hasFieldVar != null)
      {
        specificMapSizeCondition = localSpecificMapVar.ref(isSchemaTypeVarName).not().cand(localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null()));
      }
      else
      {
        specificMapSizeCondition = localSpecificMapVar.ref(rawFieldVarName).eq(JExpr._null());
      }

      specificMapSetCondition._then()._if(specificMapSizeCondition)._then().assignPlus(localSpecificMapVar.ref("__size"), JExpr.lit(1));
      specificMapSetCondition._then().assign(localSpecificMapVar.ref(coercedFieldVarName), param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(primitiveRawFieldVarName), param);
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), JExpr._null());
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(hasCoercedFieldVarName), JExpr.lit(true));
      }
      else
      {
        specificMapSetCondition._then().assign(localSpecificMapVar.ref(rawFieldVarName), param);
      }

      specificMapSetCondition._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
      specificMapSetCondition._else().add(_checkedUtilClass.staticInvoke("putWithoutChecking").arg(mapRef).arg(fieldNameExpr)
              .arg(param));
      specificMapSetCondition._else().assign(fieldVar, param);
      if (hasFieldVar != null)
      {
        specificMapSetCondition._else().assign(hasFieldVar, JExpr.lit(true));
      }
      unboxifySetter.body()._return(JExpr._this());
    }
  }

  protected void generateTyperef(JDefinedClass typerefClass, TyperefTemplateSpec typerefSpec)
  {
    typerefClass.javadoc().append(typerefSpec.getSchema().getDoc());

    setDeprecatedAnnotationAndJavadoc(typerefSpec.getSchema(), typerefClass);

    typerefClass._extends(TyperefInfo.class);

    final JVar schemaField = generateSchemaField(typerefClass, typerefSpec.getSchema(), typerefSpec.getSourceFileFormat());

    generateCustomClassInitialization(typerefClass, typerefSpec.getCustomInfo());

    final JMethod constructor = typerefClass.constructor(JMod.PUBLIC);
    constructor.body().invoke(SUPER).arg(schemaField);
  }

  protected void generateUnion(JDefinedClass unionClass, UnionTemplateSpec unionSpec)
      throws JClassAlreadyExistsException
  {
    extendUnionBaseClass(unionClass);

    final JVar schemaField = generateSchemaField(unionClass, unionSpec.getSchema(), unionSpec.getSourceFileFormat());

    // Generate instance vars for members.
    Map<String, JVar> memberVarMap = new HashMap<>();
    Map<String, JVar> hasMemberVarMap = new HashMap<>();
    Map<String, JType> memberDataClassMap = new HashMap<>();
    Map<String, JExpression> memberNameExprMap = new HashMap<>();
    for (UnionTemplateSpec.Member member : unionSpec.getMembers())
    {
      if (member.getClassTemplateSpec() != null)
      {
        final String memberName = CodeUtil.uncapitalize(CodeUtil.getUnionMemberName(member));
        JType memberType = generate(member.getClassTemplateSpec());
        JType unboxifiedMemberType = memberType.unboxify();
        final JVar memberVar;

        if (memberType.equals(unboxifiedMemberType))
        {
          memberVar = unionClass.field(JMod.PRIVATE, memberType, "_" + memberName + "Member", JExpr._null());
        }
        else
        {
          memberVar = unionClass.field(JMod.PRIVATE, unboxifiedMemberType, "_" + memberName + "Member",
              getDefaultPrimitiveExpression(unboxifiedMemberType));
          JVar hasMemberVar = unionClass.field(JMod.PRIVATE, getCodeModel().BOOLEAN,
              "_has" + StringUtils.capitalize(memberName) + "Member", JExpr.lit(false));
          hasMemberVarMap.put(member.getUnionMemberKey(), hasMemberVar);
        }

        memberVarMap.put(member.getUnionMemberKey(), memberVar);
        memberDataClassMap.put(member.getUnionMemberKey(), getDataClass(member.getSchema()));

        final JExpression fieldNameExpr = JExpr.ref("MEMBER_" + StringUtils.capitalize(memberName)).invoke("getUnionMemberKey");
        memberNameExprMap.put(member.getUnionMemberKey(), fieldNameExpr);
      }
    }

    final JClass changeListenerClass;
    final JVar changeListenerVar;

    // Generate change listener if there are any members.
    if (!memberVarMap.isEmpty())
    {
      changeListenerClass = generateChangeListener(unionClass, memberVarMap, hasMemberVarMap);
      changeListenerVar = unionClass.field(JMod.PRIVATE, changeListenerClass, "__changeListener");
    }
    else
    {
      changeListenerClass = null;
      changeListenerVar = null;
    }

    final JClass specificMapClass = generateSpecificMap(unionClass, memberVarMap, memberDataClassMap, memberNameExprMap);
    final JVar specificMapVar = unionClass.field(JMod.PRIVATE, specificMapClass, "__specificMap");
    final JClass specificMapProviderClass =
        generateSpecificMapProvider(unionClass, memberVarMap, memberDataClassMap, specificMapClass);
    unionClass.field(JMod.PUBLIC | JMod.STATIC | JMod.FINAL, specificMapProviderClass,
        "SPECIFIC_DATA_COMPLEX_PROVIDER", JExpr._new(specificMapProviderClass));

    // Default union datamap size to 1 (last arg) as union can have at-most one element.
    // We don't need cache for unions, so pass in -1 for cache size to ignore size param.
    generateDataMapConstructor(unionClass, schemaField, specificMapVar);
    generateConstructorWithObjectArg(unionClass, schemaField, changeListenerVar, specificMapVar);

    for (UnionTemplateSpec.Member member : unionSpec.getMembers())
    {
      if (member.getClassTemplateSpec() != null)
      {
        generateUnionMemberAccessors(unionClass, member, generate(member.getClassTemplateSpec()),
            schemaField, memberVarMap.get(member.getUnionMemberKey()), hasMemberVarMap.get(member.getUnionMemberKey()),
            specificMapVar);
      }
    }

    unionSpec.getMembers().stream()
        .map(UnionTemplateSpec.Member::getCustomInfo)
        .distinct()
        .forEach(customInfo -> generateCustomClassInitialization(unionClass, customInfo));

    if (_pathSpecMethods)
    {
      generatePathSpecMethodsForUnion(unionSpec, unionClass);
    }

    if (_copierMethods)
    {
      generateCopierMethods(unionClass, memberVarMap, hasMemberVarMap, changeListenerClass, specificMapVar);
    }

    if (unionSpec.getTyperefClass() != null)
    {
      final TyperefTemplateSpec typerefClassSpec = unionSpec.getTyperefClass();
      final JDefinedClass typerefInfoClass = unionClass._class(getJModValue(typerefClassSpec.getModifiers()), escapeReserved(typerefClassSpec.getClassName()));
      generateTyperef(typerefInfoClass, typerefClassSpec);

      final JFieldVar typerefInfoField = unionClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, TyperefInfo.class, DataTemplateUtil.TYPEREFINFO_FIELD_NAME);
      typerefInfoField.init(JExpr._new(typerefInfoClass));

      unionClass._implements(HasTyperefInfo.class);
      final JMethod typerefInfoMethod = unionClass.method(JMod.PUBLIC, TyperefInfo.class, "typerefInfo");
      typerefInfoMethod.body()._return(typerefInfoField);
    }
  }

  protected void extendUnionBaseClass(JDefinedClass unionClass)
  {
    unionClass._extends(_unionBaseClass);
  }

  private void generateUnionMemberAccessors(JDefinedClass unionClass, UnionTemplateSpec.Member member,
      JClass memberClass, JVar schemaField, JVar memberVar, JVar hasMemberVar, JVar specificMapVar)
  {
    final DataSchema memberType = member.getSchema();
    final String memberKey = member.getUnionMemberKey();
    final String memberRawKey = memberVar.name() + "Raw";
    final String primitiveRawMemberVarKey = memberVar.name() + "PrimitiveRaw";
    final String memberCoercedKey = memberVar.name() + "Coerced";
    final String isSchemaTypeVarName = "_isSchemaType" + StringUtils.capitalize(memberVar.name());
    final String hasMemberCoercedKey = "_has" + StringUtils.capitalize(memberVar.name()) + "Coerced";
    final String capitalizedName = CodeUtil.getUnionMemberName(member);
    final JExpression mapRef = JExpr._super().ref("_map");

    final String memberFieldName = "MEMBER_" + capitalizedName;
    final JExpression memberNameExpr = JExpr.ref(memberFieldName).invoke("getUnionMemberKey");
    final JFieldVar memberField = unionClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, UnionDataSchema.Member.class, memberFieldName);
    memberField.init(schemaField.invoke("getMemberByMemberKey").arg(memberKey));
    final String setterName = "set" + capitalizedName;

    // Generate builder.

    final String builderMethodName = (member.getAlias() != null) ? "createWith" + capitalizedName : "create";
    final JMethod createMethod = unionClass.method(JMod.PUBLIC | JMod.STATIC, unionClass, builderMethodName);
    JVar param = createMethod.param(memberClass, "value");
    final JVar newUnionVar = createMethod.body().decl(unionClass, "newUnion", JExpr._new(unionClass));
    createMethod.body().invoke(newUnionVar, setterName).arg(param);
    createMethod.body()._return(newUnionVar);

    // Is method.

    final JMethod is = unionClass.method(JMod.PUBLIC, getCodeModel().BOOLEAN, "is" + capitalizedName);
    final JBlock isBody = is.body();
    JVar localSpecificMapVar = isBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    if (hasMemberVar != null)
    {
      isBody._if(localSpecificMapVar.ne(JExpr._null()))._then()._return(
          localSpecificMapVar.ref(isSchemaTypeVarName).cor(localSpecificMapVar.ref(memberRawKey).ne(JExpr._null())));
      isBody._if(hasMemberVar)._then()._return(JExpr.lit(true));
    }
    else
    {
      isBody._if(localSpecificMapVar.ne(JExpr._null()))._then()._return(localSpecificMapVar.ref(memberRawKey).ne(JExpr._null()));
      isBody._if(memberVar.ne(JExpr._null()))._then()._return(JExpr.lit(true));
    }

    isBody._return(JExpr.invoke("memberIs").arg(memberNameExpr));

    // Getter method.

    final String getterName = "get" + capitalizedName;
    final JMethod getter = unionClass.method(JMod.PUBLIC, memberClass, getterName);
    final JBlock getterBody = getter.body();
    getterBody.invoke("checkNotNull");
    localSpecificMapVar = getterBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    JBlock specificMapNotNull = getterBody._if(localSpecificMapVar.ne(JExpr._null()))._then();
    JExpression rawValueInitExpr;
    if (hasMemberVar != null)
    {
      specificMapNotNull._if(localSpecificMapVar.ref(hasMemberCoercedKey))._then()._return(localSpecificMapVar.ref(memberCoercedKey));
      rawValueInitExpr = JOp.cond(localSpecificMapVar.ref(isSchemaTypeVarName),
          localSpecificMapVar.ref(primitiveRawMemberVarKey), localSpecificMapVar.ref(memberRawKey));
    }
    else
    {
      specificMapNotNull._if(localSpecificMapVar.ref(memberCoercedKey).ne(JExpr._null()))._then()._return(localSpecificMapVar.ref(memberCoercedKey));
      rawValueInitExpr = localSpecificMapVar.ref(memberRawKey);
    }

    JVar rawValueVar = specificMapNotNull.decl(JMod.FINAL, _objectClass, "__rawValue", rawValueInitExpr);
    specificMapNotNull._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
    specificMapNotNull.assign(localSpecificMapVar.ref(memberCoercedKey), getCoerceOutputExpression(rawValueVar, memberType, memberClass, member.getCustomInfo(), true));
    if (hasMemberVar != null)
    {
      specificMapNotNull.assign(localSpecificMapVar.ref(hasMemberCoercedKey), JExpr.lit(true));
    }
    specificMapNotNull._return(localSpecificMapVar.ref(memberCoercedKey));

    if (hasMemberVar != null)
    {
      getterBody._if(hasMemberVar)._then()._return(memberVar);
    }
    else
    {
      getterBody._if(memberVar.ne(JExpr._null()))._then()._return(memberVar);
    }

    rawValueVar =  getterBody.decl(JMod.FINAL, _objectClass, "__rawValue", mapRef.invoke("get").arg(memberNameExpr));
    getterBody._if(rawValueVar.eq(JExpr._null()))._then()._return(JExpr._null());
    getterBody.assign(memberVar, getCoerceOutputExpression(rawValueVar, memberType, memberClass, member.getCustomInfo(), true));
    getterBody._return(memberVar);

    // Setter method.

    final JMethod setter = unionClass.method(JMod.PUBLIC, Void.TYPE, setterName);
    param = setter.param(memberClass, "value");
    final JBlock setterBody = setter.body();
    setterBody.invoke("checkNotNull");
    setterBody.add(mapRef.invoke("clear"));
    localSpecificMapVar = setterBody.decl(JMod.FINAL, specificMapVar.type(), specificMapVar.name(), JExpr._this().ref(specificMapVar));
    rawValueVar = setterBody.decl(JMod.FINAL, _objectClass, "__rawValue", getCoerceInputExpression(param, memberType, member.getCustomInfo()));
    specificMapNotNull = setterBody._if(localSpecificMapVar.ne(JExpr._null()))._then();
    JConditional rawValueNotNull = specificMapNotNull._if(rawValueVar.ne(JExpr._null()));
    rawValueNotNull._then().assign(localSpecificMapVar.ref("__size"), JExpr.lit(1));
    if (hasMemberVar != null)
    {
      specificMapNotNull.assign(localSpecificMapVar.ref(memberRawKey), JExpr._null());
      rawValueNotNull._then().assign(localSpecificMapVar.ref(memberCoercedKey), param);
      rawValueNotNull._then().assign(localSpecificMapVar.ref(primitiveRawMemberVarKey), param);
      rawValueNotNull._then().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(true));
      rawValueNotNull._then().assign(localSpecificMapVar.ref(hasMemberCoercedKey), JExpr.lit(true));
      rawValueNotNull._else().assign(localSpecificMapVar.ref(memberCoercedKey), getDefaultPrimitiveExpression(memberVar.type()));
      rawValueNotNull._else().assign(localSpecificMapVar.ref(primitiveRawMemberVarKey), getDefaultPrimitiveExpression(memberVar.type()));
      rawValueNotNull._else().assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(false));
      rawValueNotNull._else().assign(localSpecificMapVar.ref(hasMemberCoercedKey), JExpr.lit(false));
    }
    else
    {
      specificMapNotNull.assign(localSpecificMapVar.ref(memberCoercedKey), param);
      specificMapNotNull.assign(localSpecificMapVar.ref(memberRawKey), rawValueVar);
      specificMapNotNull.assign(localSpecificMapVar.ref(isSchemaTypeVarName), JExpr.lit(false));
    }
    specificMapNotNull._return();

    setterBody.assign(memberVar, param);
    if (hasMemberVar != null)
    {
      setterBody.assign(hasMemberVar, param.ne(JExpr._null()));
    }
    setterBody.add(_checkedUtilClass.staticInvoke("putWithoutChecking").arg(mapRef).arg(memberNameExpr)
        .arg(rawValueVar));
  }

  private void generatePathSpecMethodsForUnion(UnionTemplateSpec unionSpec, JDefinedClass unionClass)
      throws JClassAlreadyExistsException
  {
    final JDefinedClass fieldsNestedClass = generatePathSpecNestedClass(unionClass);

    for (UnionTemplateSpec.Member member : unionSpec.getMembers())
    {
      JClass fieldsRefType = _pathSpecClass;
      if (hasNestedFields(member.getSchema()))
      {
        final JClass unionMemberClass = generate(member.getClassTemplateSpec());
        fieldsRefType = getCodeModel().ref(unionMemberClass.fullName() + ".Fields");
      }

      String memberKey = member.getUnionMemberKey();
      String methodName = CodeUtil.getUnionMemberName(member);
      final JMethod accessorMethod = fieldsNestedClass.method(JMod.PUBLIC, fieldsRefType, methodName);
      accessorMethod.body()._return(JExpr._new(fieldsRefType).arg(JExpr.invoke("getPathComponents")).arg(memberKey));
    }
  }

  private void populateClassContent(ClassTemplateSpec classTemplateSpec, JDefinedClass definedClass)
      throws JClassAlreadyExistsException
  {
    if (!_generatedClasses.containsKey(definedClass))
    {
      _generatedClasses.put(definedClass, classTemplateSpec);

      JavaCodeUtil.annotate(definedClass, "Data Template", classTemplateSpec.getLocation(), _rootPath);

      if (classTemplateSpec instanceof ArrayTemplateSpec)
      {
        generateArray(definedClass, (ArrayTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof EnumTemplateSpec)
      {
        generateEnum(definedClass, (EnumTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof FixedTemplateSpec)
      {
        generateFixed(definedClass, (FixedTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof MapTemplateSpec)
      {
        generateMap(definedClass, (MapTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof RecordTemplateSpec)
      {
        generateRecord(definedClass, (RecordTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof TyperefTemplateSpec)
      {
        generateTyperef(definedClass, (TyperefTemplateSpec) classTemplateSpec);
      }
      else if (classTemplateSpec instanceof UnionTemplateSpec)
      {
        generateUnion(definedClass, (UnionTemplateSpec) classTemplateSpec);
      }
      else
      {
        throw new RuntimeException();
      }
    }
  }

  private JFieldVar generateSchemaField(JDefinedClass templateClass, DataSchema schema, SchemaFormatType sourceFormatType)
  {
    // If format is indeterminable (e.g. from IDL), then use default format
    final SchemaFormatType schemaFormatType = Optional.ofNullable(sourceFormatType).orElse(DEFAULT_SCHEMA_FORMAT_TYPE);

    final JFieldRef schemaFormatTypeRef = _schemaFormatTypeClass.staticRef(schemaFormatType.name());
    final JFieldVar schemaField = templateClass.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, schema.getClass(), DataTemplateUtil.SCHEMA_FIELD_NAME);

    // Compactly encode the schema text
    String schemaText;
    switch (schemaFormatType)
    {
      case PDSC:
        schemaText = SchemaToJsonEncoder.schemaToJson(schema, JsonBuilder.Pretty.COMPACT);
        break;
      case PDL:
        schemaText = SchemaToPdlEncoder.schemaToPdl(schema, SchemaToPdlEncoder.EncodingStyle.COMPACT);
        break;
      default:
        // This should never happen if all enum values are handled
        throw new IllegalStateException(String.format("Unrecognized schema format type '%s'", schemaFormatType));
    }

    // Generate the method invocation to parse the schema text
    final JInvocation parseSchemaInvocation = _dataTemplateUtilClass.staticInvoke("parseSchema")
        .arg(getSizeBoundStringLiteral(schemaText));

    // TODO: Eventually use new interface for all formats, postponing adoption for PDSC to avoid build failures.
    if (schemaFormatType != SchemaFormatType.PDSC)
    {
      parseSchemaInvocation.arg(schemaFormatTypeRef);
    }

    // Generate the schema field initialization
    schemaField.init(JExpr.cast(getCodeModel()._ref(schema.getClass()), parseSchemaInvocation));

    return schemaField;
  }

  private void generatePathSpecMethodsForCollection(JDefinedClass templateClass, DataSchema schema, JClass childClass, String wildcardMethodName)
      throws JClassAlreadyExistsException
  {
    if (hasNestedFields(schema))
    {
      final JDefinedClass fieldsNestedClass = generatePathSpecNestedClass(templateClass);

      final JClass itemsFieldType = getCodeModel().ref(childClass.fullName() + ".Fields");

      final JMethod constantField = fieldsNestedClass.method(JMod.PUBLIC, itemsFieldType, wildcardMethodName);
      constantField.body()._return(JExpr._new(itemsFieldType).arg(JExpr.invoke("getPathComponents")).arg(_pathSpecClass.staticRef("WILDCARD")));
    }
  }

  private JDefinedClass generatePathSpecNestedClass(JDefinedClass templateClass)
      throws JClassAlreadyExistsException
  {
    final JDefinedClass fieldsNestedClass = templateClass._class(JMod.PUBLIC | JMod.STATIC, "Fields");
    fieldsNestedClass._extends(_pathSpecClass);

    final JMethod constructor = fieldsNestedClass.constructor(JMod.PUBLIC);
    final JClass listString = getCodeModel().ref(List.class).narrow(String.class);
    final JVar namespace = constructor.param(listString, "path");
    final JVar name = constructor.param(String.class, "name");
    constructor.body().invoke(SUPER).arg(namespace).arg(name);

    fieldsNestedClass.constructor(JMod.PUBLIC).body().invoke(SUPER);
    return fieldsNestedClass;
  }

  /**
   * @see com.linkedin.data.template.Custom#initializeCustomClass(Class)
   * @see com.linkedin.data.template.Custom#initializeCoercerClass(Class)
   */
  private void generateCustomClassInitialization(JDefinedClass templateClass, CustomInfoSpec customInfo)
  {
    if (customInfo != null)
    {
      // initialize custom class
      final String customClassFullName = customInfo.getCustomClass().getNamespace() + "." + customInfo.getCustomClass().getClassName();
      templateClass.init().add(_customClass.staticInvoke("initializeCustomClass").arg(getCodeModel().ref(customClassFullName).dotclass()));

      // initialize explicitly specified coercer class
      if (customInfo.getCoercerClass() != null)
      {
        final String coercerClassFullName = customInfo.getCoercerClass().getNamespace() + "." + customInfo.getCoercerClass().getClassName();
        templateClass.init().add(_customClass.staticInvoke("initializeCoercerClass").arg(getCodeModel().ref(coercerClassFullName).dotclass()));
      }
    }
  }

  protected void generateCoercerOverrides(JDefinedClass wrapperClass,
      ClassTemplateSpec itemSpec,
      DataSchema itemSchema,
      CustomInfoSpec customInfoSpec,
      boolean tolerateNullForCoerceOutput)
  {
    JClass valueType = generate(itemSpec);

    // Generate coerce input only for direct types. Wrapped types will just call data().
    if (CodeUtil.isDirectType(itemSchema))
    {
      JMethod coerceInput = wrapperClass.method(JMod.PROTECTED, _objectClass, "coerceInput");
      JVar inputParam = coerceInput.param(valueType, "object");
      coerceInput._throws(ClassCastException.class);
      coerceInput.annotate(Override.class);
      coerceInput.body().add(
          getCodeModel().directClass(ArgumentUtil.class.getCanonicalName()).staticInvoke("notNull").arg(inputParam).arg("object"));
      coerceInput.body()._return(getCoerceInputExpression(inputParam, itemSchema, customInfoSpec));
    }

    JMethod coerceOutput = wrapperClass.method(JMod.PROTECTED, valueType, "coerceOutput");
    JVar outputParam = coerceOutput.param(_objectClass, "object");
    coerceOutput._throws(TemplateOutputCastException.class);
    coerceOutput.annotate(Override.class);
    if (tolerateNullForCoerceOutput)
    {
      coerceOutput.body()._if(outputParam.eq(JExpr._null()))._then()._return(JExpr._null());
    }

    coerceOutput.body()._return(getCoerceOutputExpression(outputParam, itemSchema, valueType, customInfoSpec, true));
  }

  private void generateConstructorWithInitialCapacity(JDefinedClass cls, JClass elementClass, JClass specificCollectionClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar initialCapacity = argConstructor.param(getCodeModel().INT, "initialCapacity");
    argConstructor.body().invoke(THIS).arg(JExpr._new(elementClass).arg(
        JExpr._new(specificCollectionClass).arg(initialCapacity)));
  }

  private void generateConstructorWithCollection(JDefinedClass cls, JClass elementClass, JClass specificValueListClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar c = argConstructor.param(_collectionClass.narrow(elementClass), "c");
    argConstructor.body().invoke(THIS).arg(JExpr._new(_dataListClass).arg(
        JExpr._new(specificValueListClass).arg(c.invoke("size"))));
    argConstructor.body().invoke("addAll").arg(c);
  }

  private void generateConstructorWithVarArgs(JDefinedClass cls, JClass elementClass, JClass specificValueListClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar first = argConstructor.param(elementClass, "first");
    final JVar rest = argConstructor.varParam(elementClass, "rest");
    argConstructor.body().invoke(THIS).arg(JExpr._new(_dataListClass)
        .arg(JExpr._new(specificValueListClass).arg(rest.ref("length").plus(JExpr.lit(1)))));
    argConstructor.body().invoke("add").arg(first);
    argConstructor.body().invoke("addAll").arg(_arraysClass.staticInvoke("asList").arg(rest));
  }

  private void generateConstructorWithInitialCapacityAndLoadFactor(JDefinedClass cls, JClass specificCollectionClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar initialCapacity = argConstructor.param(getCodeModel().INT, "initialCapacity");
    final JVar loadFactor = argConstructor.param(getCodeModel().FLOAT, "loadFactor");
    argConstructor.body().invoke(THIS).arg(JExpr._new(_dataMapClass).arg(
        JExpr._new(specificCollectionClass).arg(initialCapacity).arg(loadFactor)));
  }

  private void generateConstructorWithMap(JDefinedClass cls, JClass valueClass)
  {
    final JMethod argConstructor = cls.constructor(JMod.PUBLIC);
    final JVar m = argConstructor.param(_mapClass.narrow(_stringClass, valueClass), "m");
    argConstructor.body().invoke(THIS).arg(JExpr.invoke("capacityFromSize").arg(m.invoke("size")));
    argConstructor.body().invoke("putAll").arg(m);
  }

  private JClass generateChangeListener(JDefinedClass cls,
      Map<String, JVar> fieldMap,
      Map<String, JVar> hasFieldMap) throws JClassAlreadyExistsException
  {
    final JClass changeListenerInterface = getCodeModel().ref(CheckedMap.ChangeListener.class);
    final JDefinedClass changeListenerClass = cls._class(JMod.PRIVATE | JMod.STATIC, "ChangeListener");
    changeListenerClass._implements(changeListenerInterface.narrow(String.class, Object.class));

    final JFieldVar objectRefVar = changeListenerClass.field(JMod.PRIVATE | JMod.FINAL, cls, "__objectRef");

    final JMethod constructor = changeListenerClass.constructor(JMod.PRIVATE);
    JVar refParam = constructor.param(cls, "reference");
    constructor.body().assign(objectRefVar, refParam);

    final JMethod method = changeListenerClass.method(JMod.PUBLIC, void.class, "onUnderlyingMapChanged");
    method.annotate(Override.class);
    final JVar keyParam = method.param(String.class, "key");
    method.param(_objectClass, "value");
    JSwitch keySwitch = method.body()._switch(keyParam);
    fieldMap.forEach((key, field) -> {
      JCase keyCase = keySwitch._case(JExpr.lit(key));
      JVar hasFieldVar = hasFieldMap.get(key);
      if (hasFieldVar != null)
      {
        keyCase.body().assign(objectRefVar.ref(field.name()), getDefaultPrimitiveExpression(field.type()));
        keyCase.body().assign(objectRefVar.ref(hasFieldVar.name()), JExpr.lit(false));
      }
      else
      {
        keyCase.body().assign(objectRefVar.ref(field.name()), JExpr._null());
      }

      keyCase.body()._break();
    });

    return changeListenerClass;
  }

  private JClass generateSpecificListProvider(JDefinedClass arrayClass,
      JClass specificListClass,
      JClass elementClass,
      JClass elementDataClass) throws JClassAlreadyExistsException
  {
    final JClass providerInterface = getCodeModel().ref(SpecificDataComplexProvider.class);
    final JDefinedClass providerClass = arrayClass._class(JMod.PRIVATE | JMod.STATIC, "SpecificListProvider");
    providerClass._implements(providerInterface);

    // Implement getList
    JMethod getList = providerClass.method(JMod.PUBLIC, _listClass.narrow(Object.class), "getList");
    getList.annotate(Override.class);
    getList.body()._return(JExpr._new(specificListClass));

    // Implement getDataListWithCapacity
    JMethod getListWithCapacity = providerClass.method(JMod.PUBLIC, _listClass.narrow(Object.class), "getList");
    getListWithCapacity.annotate(Override.class);
    JVar capacityParam = getListWithCapacity.param(getCodeModel().INT, "__capacity");
    getListWithCapacity.body()._return(JExpr._new(specificListClass).arg(capacityParam));

    // Implement getChild
    JMethod getChild = providerClass.method(JMod.PUBLIC, providerInterface, "getChild");
    getChild.annotate(Override.class);
    if (elementDataClass != null && (elementDataClass.equals(_dataMapClass) || elementDataClass.equals(_dataListClass)))
    {
      getChild.body()._return(elementClass.staticRef("SPECIFIC_DATA_COMPLEX_PROVIDER"));
    }
    else
    {
      getChild.body()._return(providerInterface.staticRef("DEFAULT"));
    }

    return providerClass;
  }

  private JClass generateSpecificMapProvider(JDefinedClass mapClass,
      JClass specificMapClass,
      JClass valueClass,
      JClass valueDataClass) throws JClassAlreadyExistsException
  {
    final JClass providerInterface = getCodeModel().ref(SpecificDataComplexProvider.class);
    final JDefinedClass providerClass = mapClass._class(JMod.PRIVATE | JMod.STATIC, "SpecificMapProvider");
    providerClass._implements(providerInterface);

    // Implement getMap
    JMethod getMap = providerClass.method(JMod.PUBLIC, _mapClass.narrow(String.class, Object.class), "getMap");
    getMap.annotate(Override.class);
    getMap.body()._return(JExpr._new(specificMapClass));

    // Implement getMap with capacity
    JMethod getMapWithCapacity = providerClass.method(JMod.PUBLIC, _mapClass.narrow(String.class, Object.class), "getMap");
    getMapWithCapacity.annotate(Override.class);
    JVar capacity = getMapWithCapacity.param(getCodeModel().INT, "__capacity");
    getMapWithCapacity.body()._return(JExpr._new(specificMapClass).arg(capacity));

    // Implement getChild
    JMethod getChild = providerClass.method(JMod.PUBLIC, providerInterface, "getChild");
    getChild.annotate(Override.class);
    getChild.param(_stringClass, "__key");
    if (valueDataClass != null && (valueDataClass.equals(_dataMapClass) || valueDataClass.equals(_dataListClass)))
    {
      getChild.body()._return(valueClass.staticRef("SPECIFIC_DATA_COMPLEX_PROVIDER"));
    }
    else
    {
      getChild.body()._return(providerInterface.staticRef("DEFAULT"));
    }

    return providerClass;
  }

  private JClass generateSpecificMapProvider(JDefinedClass cls,
      Map<String, JVar> fieldMap,
      Map<String, JType> fieldDataClassMap,
      JClass specificMapClass) throws JClassAlreadyExistsException
  {
    final JClass providerInterface = getCodeModel().ref(SpecificDataComplexProvider.class);
    final JDefinedClass providerClass = cls._class(JMod.PRIVATE | JMod.STATIC, "SpecificMapProvider");
    providerClass._implements(providerInterface);

    // Implement getMap
    JMethod getMap = providerClass.method(JMod.PUBLIC, _mapClass.narrow(String.class, Object.class), "getMap");
    getMap.annotate(Override.class);
    getMap.body()._return(JExpr._new(specificMapClass));

    // Implement getMapWithCapacity
    JMethod getMapWithCapacity = providerClass.method(JMod.PUBLIC, _mapClass.narrow(String.class, Object.class), "getMap");
    getMapWithCapacity.annotate(Override.class);
    getMapWithCapacity.param(getCodeModel().INT, "__capacity");
    getMapWithCapacity.body()._return(JExpr._new(specificMapClass));

    // Implement getChild
    JMethod getChild = providerClass.method(JMod.PUBLIC, providerInterface, "getChild");
    getChild.annotate(Override.class);
    JVar keyParam = getChild.param(_stringClass, "__key");
    long dataComplexChildCount = fieldDataClassMap.values().stream().filter(klass ->
        klass.equals(_dataMapClass) || klass.equals(_dataListClass)).count();
    if (dataComplexChildCount > 0)
    {
      JSwitch keySwitch = getChild.body()._switch(keyParam);
      fieldMap.forEach((key, value) ->
      {
        JType dataClass = fieldDataClassMap.get(key);
        if (dataClass == null || !(dataClass.equals(_dataMapClass) || dataClass.equals(_dataListClass)))
        {
          return;
        }

        JCase keyCase = keySwitch._case(JExpr.lit(key));
        keyCase.body()._return(value.type().boxify().staticRef("SPECIFIC_DATA_COMPLEX_PROVIDER"));
      });
      keySwitch._default().body()._return(providerClass.staticRef("DEFAULT"));
    }
    else
    {
      getChild.body()._return(providerInterface.staticRef("DEFAULT"));
    }

    return providerClass;
  }

  private JClass generateSpecificMap(JDefinedClass cls,
      Map<String, JVar> fieldMap,
      Map<String, JType> fieldDataClassMap,
      Map<String, JExpression> fieldNameExprMap) throws JClassAlreadyExistsException
  {
    final JDefinedClass specificMapClass = cls._class(JMod.PRIVATE | JMod.STATIC, "SpecificMap");
    specificMapClass._extends(getCodeModel().ref(SpecificDataTemplateSchemaMap.class));

    Map<String, JVar> localVarMap = new HashMap<>(fieldMap.size());
    Map<String, JVar> localPrimitiveVarMap = new HashMap<>(fieldMap.size());
    Map<String, JVar> localCoercedVarMap = new HashMap<>(fieldMap.size());
    Map<String, JVar> localCoercedHasVarMap = new HashMap<>(fieldMap.size());
    Map<String, JVar> isSchemaTypeVarMap = new HashMap<>(fieldMap.size());
    fieldMap.forEach((key, value) -> {
      JFieldVar localVar = specificMapClass.field(JMod.PRIVATE, Object.class, value.name() + "Raw");
      localVarMap.put(key, localVar);

      JFieldVar localCoercedVar = specificMapClass.field(JMod.PRIVATE, value.type(), value.name() + "Coerced");
      localCoercedVarMap.put(key, localCoercedVar);

      JType boxedValueType = value.type().boxify();
      if (!boxedValueType.equals(value.type()))
      {
        JFieldVar localCoercedHasVar = specificMapClass.field(JMod.PRIVATE, getCodeModel().BOOLEAN,
            "_has" + StringUtils.capitalize(value.name()) + "Coerced");
        localCoercedHasVarMap.put(key, localCoercedHasVar);

        JFieldVar localPrimitiveVar = specificMapClass.field(JMod.PRIVATE, value.type(), value.name() + "PrimitiveRaw");
        localPrimitiveVarMap.put(key, localPrimitiveVar);
      }

      JFieldVar isSchemaTypeVar = specificMapClass.field(JMod.PRIVATE, getCodeModel().BOOLEAN,
          "_isSchemaType" + StringUtils.capitalize(value.name()));
      isSchemaTypeVarMap.put(key, isSchemaTypeVar);
    });

    JFieldVar sizeVar = specificMapClass.field(JMod.PRIVATE, getCodeModel().INT, "__size");

    // Generate specificSize
    JMethod specificSize = specificMapClass.method(JMod.PROTECTED, getCodeModel().INT, "specificSize");
    specificSize.annotate(Override.class);
    specificSize.body()._return(sizeVar);

    // Generate specificContainsValue
    JMethod specificContainsValue = specificMapClass.method(JMod.PROTECTED, getCodeModel().BOOLEAN, "specificContainsValue");
    specificContainsValue.annotate(Override.class);
    JVar valueParam = specificContainsValue.param(Object.class, "__value");
    localVarMap.forEach((key, value) -> {
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      if (localPrimitiveVar != null)
      {
        specificContainsValue.body()._if(isSchemaTypeVarMap.get(key).cand(valueParam.ne(JExpr._null())).cand(valueParam.invoke("equals").arg(localPrimitiveVar)))._then()._return(JExpr.lit(true));
      }
      specificContainsValue.body()._if(value.ne(JExpr._null()).cand(value.invoke("equals").arg(valueParam)))._then()._return(JExpr.lit(true));
    });
    specificContainsValue.body()._return(JExpr.lit(false));

    // Generate specificGet
    JMethod specificGet = specificMapClass.method(JMod.PROTECTED, Object.class, "specificGet");
    specificGet.annotate(Override.class);
    JVar rawKeyParam = specificGet.param(_stringClass, "__key");
    JSwitch getSwitch = specificGet.body()._switch(rawKeyParam);
    localVarMap.forEach((key, localVar) -> {
      JCase keyCase = getSwitch._case(JExpr.lit(key));
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      if (localPrimitiveVar != null)
      {
        keyCase.body()._return(JOp.cond(isSchemaTypeVarMap.get(key), localPrimitiveVar, localVar));
      }
      else
      {
        keyCase.body()._return(localVar);
      }
    });
    getSwitch._default().body()._return(JExpr._null());

    // Generate specificPut
    JMethod specificPut = specificMapClass.method(JMod.PROTECTED, Object.class, "specificPut");
    specificPut.annotate(Override.class);
    JVar keyParam = specificPut.param(String.class, "__key");
    JVar putValueParam = specificPut.param(Object.class, "__value");
    JSwitch putSwitch = specificPut.body()._switch(keyParam);
    localVarMap.forEach((key, localVar) -> {
      JCase keyCase = putSwitch._case(JExpr.lit(key));
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      JVar previous;
      if (localPrimitiveVar != null)
      {
        previous = keyCase.body().decl(getCodeModel().ref(Object.class), "__previous__", JOp.cond(isSchemaTypeVarMap.get(key), localPrimitiveVar, localVar));
      }
      else
      {
        previous = keyCase.body().decl(getCodeModel().ref(Object.class), "__previous__", localVar);
      }

      JType dataClass = fieldDataClassMap.get(key);
      JVar isSchemaTypeVar = isSchemaTypeVarMap.get(key);
      JVar coercedVar = localCoercedVarMap.get(key);
      JVar hasCoercedVar = localCoercedHasVarMap.get(key);
      if (dataClass != null)
      {
        keyCase.body().assign(isSchemaTypeVar, putValueParam._instanceof(dataClass));
      }
      else
      {
        keyCase.body().assign(isSchemaTypeVar, JExpr.lit(true));
      }

      if (localPrimitiveVar != null)
      {
        JConditional isSameSchemaType = keyCase.body()._if(isSchemaTypeVar);
        isSameSchemaType._then().assign(localPrimitiveVar, JExpr.cast(localPrimitiveVar.type(), putValueParam));
        isSameSchemaType._then().assign(localVar, JExpr._null());
        isSameSchemaType._then().assign(hasCoercedVar, JExpr.lit(true));
        isSameSchemaType._then().assign(coercedVar, localPrimitiveVar);
        isSameSchemaType._else().assign(localPrimitiveVar, getDefaultPrimitiveExpression(localPrimitiveVar.type()));
        isSameSchemaType._else().assign(localVar, putValueParam);
        isSameSchemaType._else().assign(hasCoercedVar, JExpr.lit(false));
        isSameSchemaType._else().assign(coercedVar, localPrimitiveVar);
      }
      else
      {
        keyCase.body().assign(localVar, putValueParam);
        keyCase.body().assign(coercedVar, JExpr._null());
      }

      keyCase.body()._if(previous.eq(JExpr._null()))._then().assignPlus(sizeVar, JExpr.lit(1));
      keyCase.body()._return(previous);
    });
    JBlock defaultBlock = putSwitch._default().body();
    defaultBlock._return(getCodeModel().ref(SpecificDataTemplateSchemaMap.class).staticRef("EXTRA_FIELD"));

    // Generate specificRemove
    JMethod specificRemove = specificMapClass.method(JMod.PROTECTED, Object.class, "specificRemove");
    specificRemove.annotate(Override.class);
    rawKeyParam = specificRemove.param(_stringClass, "__key");
    JSwitch removeSwitch = specificRemove.body()._switch(rawKeyParam);
    localVarMap.forEach((key, localVar) -> {
      JCase keyCase = removeSwitch._case(JExpr.lit(key));
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      JVar previous;
      if (localPrimitiveVar != null)
      {
        previous = keyCase.body().decl(getCodeModel().ref(Object.class), "__previous__", JOp.cond(isSchemaTypeVarMap.get(key), localPrimitiveVar, localVar));
        keyCase.body().assign(localPrimitiveVar, getDefaultPrimitiveExpression(localPrimitiveVar.type()));
      }
      else
      {
        previous = keyCase.body().decl(getCodeModel().ref(Object.class), "__previous__", localVar);
      }

      keyCase.body().assign(localVar, JExpr._null());
      keyCase.body().assign(isSchemaTypeVarMap.get(key), JExpr.lit(false));

      JVar coercedVar = localCoercedVarMap.get(key);
      JVar hasCoercedVar = localCoercedHasVarMap.get(key);
      if (hasCoercedVar != null)
      {
        keyCase.body().assign(coercedVar, getDefaultPrimitiveExpression(coercedVar.type()));
        keyCase.body().assign(hasCoercedVar, JExpr.lit(false));
      }
      else
      {
        keyCase.body().assign(coercedVar, JExpr._null());
      }

      keyCase.body()._if(previous.ne(JExpr._null()))._then().assignPlus(sizeVar, JExpr.lit(-1));
      keyCase.body()._return(previous);
    });
    defaultBlock = removeSwitch._default().body();
    defaultBlock._return(getCodeModel().ref(SpecificDataTemplateSchemaMap.class).staticRef("EXTRA_FIELD"));

    // Generate specificClear
    JMethod specificClear = specificMapClass.method(JMod.PROTECTED, getCodeModel().VOID, "specificClear");
    specificClear.annotate(Override.class);
    localVarMap.forEach((key, value) -> {
      specificClear.body().assign(value, JExpr._null());
      specificClear.body().assign(isSchemaTypeVarMap.get(key), JExpr.lit(false));

      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      if (localPrimitiveVar != null)
      {
        specificClear.body().assign(localPrimitiveVar, getDefaultPrimitiveExpression(localPrimitiveVar.type()));
      }

      JVar coercedVar = localCoercedVarMap.get(key);
      JVar hasCoercedVar = localCoercedHasVarMap.get(key);
      if (hasCoercedVar != null)
      {
        specificClear.body().assign(coercedVar, getDefaultPrimitiveExpression(coercedVar.type()));
        specificClear.body().assign(hasCoercedVar, JExpr.lit(false));
      }
      else
      {
        specificClear.body().assign(coercedVar, JExpr._null());
      }
    });
    specificClear.body().assign(sizeVar, JExpr.lit(0));

    // Generate specificNextEntry
    JClass entryClass = getCodeModel().ref(Map.Entry.class).narrow(String.class, Object.class);
    JMethod specificNextEntry = specificMapClass.method(JMod.PROTECTED, entryClass, "specificNextEntry");
    JVar currentEntryParam = specificNextEntry.param(entryClass, "__currentEntry__");
    JVar foundVar = specificNextEntry.body().decl(getCodeModel().BOOLEAN, "__found", currentEntryParam.eq(JExpr._null()));
    localVarMap.forEach((key, value) -> {
      JBlock ifBlock;
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      JExpression valueExpr;
      if (localPrimitiveVar != null)
      {
        ifBlock = specificNextEntry.body()._if(isSchemaTypeVarMap.get(key).cor(value.ne(JExpr._null())))._then();
        valueExpr = JOp.cond(isSchemaTypeVarMap.get(key), localPrimitiveVar, value);
      }
      else
      {
        ifBlock = specificNextEntry.body()._if(value.ne(JExpr._null()))._then();
        valueExpr = value;
      }
      ifBlock._if(foundVar)._then()._return(JExpr._new(
          getCodeModel().ref(SpecificDataTemplateSchemaMap.SpecificMapEntry.class))
          .arg(fieldNameExprMap.get(key)).arg(valueExpr).arg(JExpr._this()));

      ifBlock.assign(foundVar, fieldNameExprMap.get(key).invoke("equals").arg(currentEntryParam.invoke("getKey")));
    });
    specificNextEntry.body()._return(JExpr._null());

    // Generate specificForEach
    JMethod specificForEach = specificMapClass.method(JMod.PROTECTED, getCodeModel().VOID, "specificForEach");
    JVar actionParam = specificForEach.param(getCodeModel().ref(BiConsumer.class).narrow(String.class, Object.class), "__action");
    localVarMap.forEach((key, value) -> {

      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      JExpression condition;
      JExpression valueExpr;
      if (localPrimitiveVar != null)
      {
        condition = isSchemaTypeVarMap.get(key).cor(value.ne(JExpr._null()));
        valueExpr = JOp.cond(isSchemaTypeVarMap.get(key), localPrimitiveVar, value);
      }
      else
      {
        condition = value.ne(JExpr._null());
        valueExpr = value;
      }

      specificForEach.body()
          ._if(condition)
          ._then()
          .invoke(actionParam, "accept").arg(fieldNameExprMap.get(key)).arg(valueExpr);
    });

    // Generate specificTraverse
    JMethod specificTraverse = specificMapClass.method(JMod.PROTECTED, getCodeModel().VOID, "specificTraverse");
    JVar callbackParam = specificTraverse.param(getCodeModel().ref(Data.TraverseCallback.class), "__callback");
    JVar cycleCheckerParam = specificTraverse.param(getCodeModel().ref(Data.CycleChecker.class), "__cycleChecker");
    specificTraverse._throws(IOException.class);
    localVarMap.forEach((key, value) -> {
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      JExpression condition;
      if (localPrimitiveVar != null)
      {
        condition = isSchemaTypeVarMap.get(key).cor(value.ne(JExpr._null()));
      }
      else
      {
        condition = value.ne(JExpr._null());
      }

      JBlock notNull = specificTraverse.body()._if(condition)._then();
      notNull.add(callbackParam.invoke("key").arg(fieldNameExprMap.get(key)));
      addCallbackInvocation(notNull, isSchemaTypeVarMap.get(key), fieldDataClassMap.get(key), value, localPrimitiveVar, callbackParam, cycleCheckerParam);
    });

    // Generate specificCopy
    JMethod specificCopy = specificMapClass.method(JMod.PROTECTED, getCodeModel().ref(SpecificDataTemplateSchemaMap.class), "specificCopy");
    specificCopy._throws(CloneNotSupportedException.class);
    JVar copyVar = specificCopy.body().decl(specificMapClass, "__copy", JExpr._new(specificMapClass));
    localVarMap.forEach((key, value) -> {
      specificCopy.body().assign(copyVar.ref(value.name()), JExpr.invoke("copy").arg(value));
      JVar localPrimitiveVar = localPrimitiveVarMap.get(key);
      if (localPrimitiveVar != null)
      {
        specificCopy.body().assign(copyVar.ref(localPrimitiveVar.name()), localPrimitiveVar);
      }
      JVar isSchemaTypeVar = isSchemaTypeVarMap.get(key);
      specificCopy.body().assign(copyVar.ref(isSchemaTypeVar.name()), isSchemaTypeVar);
    });
    specificCopy.body().assign(copyVar.ref(sizeVar.name()), sizeVar);
    specificCopy.body()._return(copyVar);

    return specificMapClass;
  }

  private void addCallbackInvocation(JBlock block, JVar isSchemaTypeValue, JType dataClassType, JVar value,
      JVar primitiveValue, JVar callbackParam, JVar cycleCheckerParam)
  {
    if (dataClassType == null)
    {
      block.staticInvoke(getCodeModel().ref(Data.class), "traverse").arg(value).arg(callbackParam).arg(cycleCheckerParam);
      return;
    }

    JConditional schemaMatch = block._if(isSchemaTypeValue);
    JExpression castVar;

    if (primitiveValue != null)
    {
      castVar = primitiveValue;
    }
    else
    {
      castVar = JExpr.cast(dataClassType, value);
    }

    switch (dataClassType.fullName())
    {
      case "java.lang.String":
        schemaMatch._then().invoke(callbackParam, "stringValue").arg(castVar);
        break;
      case "java.lang.Integer":
        schemaMatch._then().invoke(callbackParam, "integerValue").arg(castVar);
        break;
      case "com.linkedin.data.DataMap":
      case "com.linkedin.data.DataList":
        schemaMatch._then().invoke(castVar, "traverse").arg(callbackParam).arg(cycleCheckerParam);
        break;
      case "java.lang.Boolean":
        schemaMatch._then().invoke(callbackParam, "booleanValue").arg(castVar);
        break;
      case "java.lang.Long":
        schemaMatch._then().invoke(callbackParam, "longValue").arg(castVar);
        break;
      case "java.lang.Float":
        schemaMatch._then().invoke(callbackParam, "floatValue").arg(castVar);
        break;
      case "java.lang.Double":
        schemaMatch._then().invoke(callbackParam, "doubleValue").arg(castVar);
        break;
      case "com.linkedin.data.ByteString":
        schemaMatch._then().invoke(callbackParam, "byteStringValue").arg(castVar);
        break;
      default:
        throw new IllegalStateException("Unknown data class name: " + dataClassType.fullName());
    }

    schemaMatch._else().staticInvoke(getCodeModel().ref(Data.class), "traverse").arg(value).arg(callbackParam).arg(cycleCheckerParam);
  }

  private JExpression getCoerceOutputExpression(JExpression rawExpr, DataSchema schema, JClass typeClass,
      CustomInfoSpec customInfoSpec, boolean isRawExprNonNull)
  {
    if (CodeUtil.isDirectType(schema))
    {
      if (customInfoSpec == null)
      {
        switch (schema.getDereferencedType())
        {
          case INT:
            return _dataTemplateUtilClass.staticInvoke("coerceIntOutput").arg(rawExpr);
          case FLOAT:
            return _dataTemplateUtilClass.staticInvoke("coerceFloatOutput").arg(rawExpr);
          case LONG:
            return _dataTemplateUtilClass.staticInvoke("coerceLongOutput").arg(rawExpr);
          case DOUBLE:
            return _dataTemplateUtilClass.staticInvoke("coerceDoubleOutput").arg(rawExpr);
          case BYTES:
            return _dataTemplateUtilClass.staticInvoke("coerceBytesOutput").arg(rawExpr);
          case BOOLEAN:
            return _dataTemplateUtilClass.staticInvoke("coerceBooleanOutput").arg(rawExpr);
          case STRING:
            return _dataTemplateUtilClass.staticInvoke("coerceStringOutput").arg(rawExpr);
          case ENUM:
            return _dataTemplateUtilClass.staticInvoke("coerceEnumOutput")
                .arg(rawExpr)
                .arg(typeClass.dotclass())
                .arg(typeClass.staticRef(DataTemplateUtil.UNKNOWN_ENUM));
        }
      }

      JClass customClass = generate(customInfoSpec.getCustomClass());
      return _dataTemplateUtilClass.staticInvoke("coerceCustomOutput").arg(rawExpr).arg(customClass.dotclass());
    }
    else
    {
      JExpression returnExpression;
      switch (schema.getDereferencedType())
      {
        case MAP:
        case RECORD:
          returnExpression = JExpr._new(typeClass)
              .arg(_dataTemplateUtilClass.staticInvoke("castOrThrow").arg(rawExpr).arg(_dataMapClass.dotclass()));
          break;
        case ARRAY:
          returnExpression = JExpr._new(typeClass)
              .arg(_dataTemplateUtilClass.staticInvoke("castOrThrow").arg(rawExpr).arg(_dataListClass.dotclass()));
          break;
        case FIXED:
        case UNION:
          returnExpression = JExpr._new(typeClass).arg(rawExpr);
          break;
        default:
          throw new TemplateOutputCastException(
              "Cannot handle wrapped schema of type " + schema.getDereferencedType());
      }

      return isRawExprNonNull ? returnExpression : JOp.cond(rawExpr.eq(JExpr._null()), JExpr._null(), returnExpression);
    }
  }

  private JExpression getDefaultPrimitiveExpression(JType type)
  {
    if (!type.isPrimitive())
    {
      throw new IllegalArgumentException("Type: " + type + " is not a primitive type");
    }

    if (getCodeModel().INT.equals(type) ||
        getCodeModel().LONG.equals(type) ||
        getCodeModel().FLOAT.equals(type) ||
        getCodeModel().DOUBLE.equals(type))
    {
      return JExpr.lit(0);
    }

    if (getCodeModel().BOOLEAN.equals(type))
    {
      return JExpr.lit(false);
    }

    throw new IllegalArgumentException("Unsupported primitive type: " + type);
  }

  private JExpression getCoerceInputExpression(JExpression objectExpr, DataSchema schema, CustomInfoSpec customInfoSpec)
  {
    if (CodeUtil.isDirectType(schema))
    {
      if (customInfoSpec == null)
      {
        switch (schema.getDereferencedType())
        {
          case INT:
          case FLOAT:
          case LONG:
          case DOUBLE:
          case BYTES:
          case BOOLEAN:
          case STRING:
            return objectExpr;
          case ENUM:
            return objectExpr.invoke("toString");
        }
      }

      JClass customClass = generate(customInfoSpec.getCustomClass());
      return _dataTemplateUtilClass.staticInvoke("coerceCustomInput").arg(objectExpr).arg(customClass.dotclass());
    }
    else
    {
      return objectExpr.invoke("data");
    }
  }

  public static class Config
  {
    private String _defaultPackage;
    private boolean _recordFieldAccessorWithMode;
    private boolean _recordFieldRemove;
    private boolean _pathSpecMethods;
    private boolean _copierMethods;
    private String _rootPath;

    public Config()
    {
      _defaultPackage = null;
      _recordFieldAccessorWithMode = true;
      _recordFieldRemove = true;
      _pathSpecMethods = true;
      _copierMethods = true;
      _rootPath = null;
    }

    public void setDefaultPackage(String defaultPackage)
    {
      _defaultPackage = defaultPackage;
    }

    public String getDefaultPackage()
    {
      return _defaultPackage;
    }

    public void setRecordFieldAccessorWithMode(boolean recordFieldAccessorWithMode)
    {
      _recordFieldAccessorWithMode = recordFieldAccessorWithMode;
    }

    public boolean getRecordFieldAccessorWithMode()
    {
      return _recordFieldAccessorWithMode;
    }

    public void setRecordFieldRemove(boolean recordFieldRemove)
    {
      _recordFieldRemove = recordFieldRemove;
    }

    public boolean getRecordFieldRemove()
    {
      return _recordFieldRemove;
    }

    public void setPathSpecMethods(boolean pathSpecMethods)
    {
      _pathSpecMethods = pathSpecMethods;
    }

    public boolean getPathSpecMethods()
    {
      return _pathSpecMethods;
    }

    public void setCopierMethods(boolean copierMethods)
    {
      _copierMethods = copierMethods;
    }

    public boolean getCopierMethods()
    {
      return _copierMethods;
    }

    public void setRootPath(String rootPath)
    {
      _rootPath = rootPath;
    }

    public String getRootPath()
    {
      return _rootPath;
    }
  }
}
