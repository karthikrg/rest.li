package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.Name;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.StringDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import java.util.ArrayList;
import java.util.List;


/**
 * Util for making data schema copioes
 */
public class CopySchemaUtil
{
  /**
   * Create a skeleton schema from the given schema
   * For example, if the given schema is a {@link RecordDataSchema}, the skeletonSchema will be an empty {@link RecordDataSchema} with no fields
   *
   * @param schema
   * @return
   * @throws CloneNotSupportedException
   */
  public static DataSchema buildSkeletonSchema(DataSchema schema) throws CloneNotSupportedException
  {
    DataSchema newSchema = null;
    switch (schema.getType())
    {
      case RECORD:
        RecordDataSchema newRecordSchema = new RecordDataSchema(new Name(((RecordDataSchema) schema).getFullName()),
                                         RecordDataSchema.RecordType.RECORD);
        RecordDataSchema originalRecordSchema = (RecordDataSchema) schema;
        if (originalRecordSchema.getAliases() != null)
        {
          newRecordSchema.setAliases(originalRecordSchema.getAliases());
        }
        if (originalRecordSchema.getDoc() != null)
        {
          newRecordSchema.setDoc(originalRecordSchema.getDoc());
        }
        if (originalRecordSchema.getProperties() != null)
        {
          newRecordSchema.setProperties(originalRecordSchema.getProperties());
        }
        newSchema = newRecordSchema;
        break;
      case UNION:
        UnionDataSchema newUnionDataSchema = new UnionDataSchema();
        UnionDataSchema unionDataSchema = (UnionDataSchema) schema;
        if (unionDataSchema.getProperties() != null)
        {
          newUnionDataSchema.setProperties(unionDataSchema.getProperties());
        }
        newSchema = newUnionDataSchema;
        break;
      case TYPEREF:
        TyperefDataSchema originalTypeRefSchema = (TyperefDataSchema) schema;
        TyperefDataSchema newTypeRefSchema = new TyperefDataSchema(new Name(originalTypeRefSchema.getFullName()));
        if (originalTypeRefSchema.getProperties() != null)
        {
          newTypeRefSchema.setProperties(originalTypeRefSchema.getProperties());
        }
        if (originalTypeRefSchema.getDoc() != null)
        {
          newTypeRefSchema.setDoc(originalTypeRefSchema.getDoc());
        }
        if (originalTypeRefSchema.getAliases() != null)
        {
          newTypeRefSchema.setAliases(originalTypeRefSchema.getAliases());
        }
        newSchema = newTypeRefSchema;
        break;
      case ARRAY:
        ArrayDataSchema originalArrayDataSchema = (ArrayDataSchema) schema;
        //Set null for this skeleton
        ArrayDataSchema newArrayDataSchema = new ArrayDataSchema(DataSchemaConstants.NULL_DATA_SCHEMA);
        if (originalArrayDataSchema.getProperties() != null)
        {
          newArrayDataSchema.setProperties(originalArrayDataSchema.getProperties());
        }
        newSchema = newArrayDataSchema;
        break;
      case MAP:
        MapDataSchema originalMapDataSchema = (MapDataSchema) schema;
        //Set null for this skeleton
        MapDataSchema newMapDataSchema = new MapDataSchema(DataSchemaConstants.NULL_DATA_SCHEMA);
        if (originalMapDataSchema.getProperties() != null)
        {
          newMapDataSchema.setProperties(originalMapDataSchema.getProperties());
        }
        newSchema = newMapDataSchema;
        break;
      case FIXED:
      case ENUM:
      default:
        // Primitive types, FIXED, ENUM: using schema's clone method
        newSchema = schema.clone();
        break;
    }
    return newSchema;
  }

  /**
   * Make a shallow copy of the schema
   * For {@link RecordDataSchema} also make shallow copy for its fields.
   * For {@link UnionDataSchema} also make shallow copy for its union members.
   *
   * @param schema
   * @return
   * @throws CloneNotSupportedException
   */
  static DataSchema getSchemaShallowCopy(DataSchema schema) throws CloneNotSupportedException
  {
    DataSchema newSchema = null;
    switch (schema.getType())
    {
      case RECORD:
        // shallow copy the recordDataSchema, but also shallow copy the fields.
        newSchema = schema.clone();
        List<RecordDataSchema.Field> fields = ((RecordDataSchema) newSchema).getFields();
        List<RecordDataSchema.Field> newFields = new ArrayList<>();
        for (RecordDataSchema.Field field: fields)
        {
          newFields.add(field.clone());
        }
        ((RecordDataSchema) newSchema).setFields(newFields, new StringBuilder());
        break;
      case UNION:
        // shallow copy UnionDataSchema, but also shallow copy the members
        newSchema = schema.clone();
        List<UnionDataSchema.Member> members = ((UnionDataSchema) newSchema).getMembers();
        List<UnionDataSchema.Member> newMembers = new ArrayList<>();
        for (UnionDataSchema.Member member: members)
        {
          newMembers.add(member.clone());

        }
        ((UnionDataSchema) newSchema).setMembers(newMembers, new StringBuilder());
        break;
      case TYPEREF:
      case FIXED:
      case ENUM:
      case ARRAY:
        newSchema = schema.clone();
        break;
      case MAP:
        newSchema = schema.clone();
        ((MapDataSchema) newSchema).setKey(new StringDataSchema());
        break;
      default:
        assert (schema.isPrimitive());
        newSchema = schema.clone();
        break;
    }
    return newSchema;
  }

  public static RecordDataSchema.Field copyField(RecordDataSchema.Field originalField, DataSchema fieldSchemaToReplace,
                                                 DataSchema recordSchemaToReplace)
  {
    RecordDataSchema.Field newField = new RecordDataSchema.Field(fieldSchemaToReplace);
    if (originalField.getAliases() != null)
    {
      // No errors are expected here, as the new schema is merely subset of the original
      newField.setAliases(originalField.getAliases(), new StringBuilder());
    }
    if (originalField.getDefault() != null)
    {
      newField.setDefault(originalField.getDefault());
    }
    if (originalField.getDoc() != null)
    {
      newField.setDoc(originalField.getDoc());
    }
    if (originalField.getName() != null)
    {
      // No errors are expected here, as the new schema is merely subset of the original
      newField.setName(originalField.getName(), new StringBuilder());
    }
    if (originalField.getOrder() != null)
    {
      newField.setOrder(originalField.getOrder());
    }
    if (originalField.getProperties() != null)
    {
      newField.setProperties(originalField.getProperties());
    }
    newField.setRecord((RecordDataSchema) recordSchemaToReplace);
    newField.setOptional(originalField.getOptional());
    return newField;
  }

  public static UnionDataSchema.Member copyUnionMember(UnionDataSchema.Member member,
                                                       DataSchema newSkeletonSchema)
  {
    UnionDataSchema.Member newMember = new UnionDataSchema.Member(newSkeletonSchema);
    if (member.hasAlias())
    {
      newMember.setAlias(member.getAlias(), new StringBuilder());
    }
    newMember.setDeclaredInline(member.isDeclaredInline());
    newMember.setDoc(member.getDoc());
    newMember.setProperties(member.getProperties());
    return newMember;
  }
}
