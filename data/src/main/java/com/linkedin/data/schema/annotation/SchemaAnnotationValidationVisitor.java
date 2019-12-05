package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.EnumDataSchema;
import com.linkedin.data.schema.FixedDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.SchemaVisitor;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.TraverserContext;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.VisitorContext;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser.VisitorTraversalResult;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.AnnotationValidationResult;
import java.util.ArrayList;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SchemaAnnotationValidationVisitor implements SchemaVisitor
{


  public SchemaAnnotationValidationVisitor(SchemaAnnotationHandler schemaAnnotationHandler)
  {
    _schemaAnnotationHandler = schemaAnnotationHandler;
  }

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order)
  {
    if (order == DataSchemaTraverse.Order.POST_ORDER)
    {
      //Skip post order
      return;
    }
    DataSchema schema = context.getCurrentSchema();
    SchemaAnnotationHandler.ValidationMetaData metaData = new SchemaAnnotationHandler.ValidationMetaData();
    boolean isLeaf = (schema.isPrimitive() || (schema instanceof EnumDataSchema) ||
                                  (schema instanceof FixedDataSchema));

    metaData.setIsLeafDataSchema(isLeaf);
    AnnotationValidationResult annotationValidationResult = _schemaAnnotationHandler.validate(
        new ArrayList<>(Arrays.asList(context.getSchemaPathSpec().toArray(new String[0]))),
        schema.getResolvedProperties(), schema, metaData);
    if (!annotationValidationResult.isValid())
    {
      // merge messages
      getVisitorTraversalResult().addMessages(context.getSchemaPathSpec(), annotationValidationResult.getMessages());
    }
  }

  @Override
  public DataSchemaRichContextTraverser.VisitorContext getInitialVisitorContext()
  {
    return new VisitorContext(){};
  }

  @Override
  public VisitorTraversalResult getVisitorTraversalResult()
  {
    return _visitorTraversalResult;
  }


  public SchemaAnnotationHandler getSchemaAnnotationHandler()
  {
    return _schemaAnnotationHandler;
  }

  private final VisitorTraversalResult _visitorTraversalResult = new VisitorTraversalResult();
  private final SchemaAnnotationHandler _schemaAnnotationHandler;
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaAnnotationValidationVisitor.class);

}
