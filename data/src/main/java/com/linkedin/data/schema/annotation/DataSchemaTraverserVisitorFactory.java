package com.linkedin.data.schema.annotation;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.SchemaVisitor;


/**
 * Factory for creating a {@link SchemaVisitor} implementation instance
 */
public class DataSchemaTraverserVisitorFactory
{
  public static SchemaVisitor createRichContextTraverserVisitor(SchemaAnnotationHandler handler)
  {
    return new PathSpecBasedSchemaAnnotationVisitor(handler);

  }
}
