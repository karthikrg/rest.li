package com.linkedin.data.schema.annotation;

import com.linkedin.data.TestUtil;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.JsonBuilder;
import com.linkedin.data.schema.SchemaToJsonEncoder;
import com.linkedin.data.template.DataTemplateUtil;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.testng.annotations.Test;


public class FakeTestSchemaAnnotationProcessor {

  @Test
  public void mytest2() throws Exception {
    String serializedSchema = "{\"type\":\"record\",\"name\":\"SampleRecord\",\"namespace\":\"com.linkedin.security.test\",\"fields\":[{\"name\":\"testField1\",\"type\":\"string\",\"domainTypes\":[\"Member.VanityName\"]},{\"name\":\"testField2\",\"type\":{\"type\":\"array\",\"items\":\"int\"},\"domainTypes\":{\"/*\":[\"Position.NonIterableId\",\"Education.NonIterableId\"]}},{\"name\":\"nestedRecordField\",\"type\":\"SampleRecord\"}]}";
    DataSchema myschema = DataTemplateUtil.parseSchema(serializedSchema);
    String test2 =  SchemaToJsonEncoder.schemaToJson(myschema, JsonBuilder.Pretty.COMPACT);
    System.out.println(test2);

  }

  @Test
  public void mytest() throws Exception{
    String moduleDir = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath().toString();
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/0_basecase.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/0_base_recursive_1.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/0_base_recursive_2.pdl";
    String filePath = moduleDir +
                      "com/linkedin/data/schema/annotation/denormalizedsource/0_base_recursive_overrides.pdl";

//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_2_cyclic_simple_overrides_invalid.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid_complex.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid_deep.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_4_cyclic_cross_ref_invalid.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/5_pathSpec_invalid.pdl";

//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/0_basecase.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/amIwrong.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid.pdl";

//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/0_simpleoverrides_2.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/1_0_multiplereference.pdl";
//      String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/1_0_multiplereference.pdl";
//      String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/1_1_testnestedshallowcopy.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_1_1_map.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_1_2_array.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_1_3_union.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_1_fixed.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_2_enum.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_3_typeref.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_4_includes.pdl";

//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_1_cyclic_simple_valid.pdl";

//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_2_cyclic_simple_overrides_invalid.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_3_cyclic_random_test_fake.pdl";
//      String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_2_cyclic_multiplefields.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_3_cyclic_external_ref_valid.pdl";
//      String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_3_cyclic_random_test_fake.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_2_enum.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/2_2_4_includes.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/4_1_comprehensive_example.pdl";
//    String filePath = moduleDir + "com/linkedin/data/schema/annotation/denormalizedsource/3_2_cyclic_simple_overrides_invalid.pdl";
    String pdlStr = new String(Files.readAllBytes(Paths.get(filePath)));
    DataSchema dataSchema = TestUtil.dataSchemaFromPdlString(pdlStr);

    PegasusSchemaAnnotationHandlerImpl customAnnotationHandler = new PegasusSchemaAnnotationHandlerImpl("customAnnotation");
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult result =
        SchemaAnnotationProcessor.process(Arrays.asList(customAnnotationHandler), dataSchema,
                                          new SchemaAnnotationProcessor.AnnotationProcessOption());
    DataSchema processedDataSchema = result.getResultSchema();
    ResolvedPropertiesReaderVisitor resolvedPropertiesReaderVisitor = new ResolvedPropertiesReaderVisitor();
    DataSchemaRichContextTraverser traverser = new DataSchemaRichContextTraverser(resolvedPropertiesReaderVisitor);
    traverser.traverse(processedDataSchema);

//    Map<String, Object> resolvedProperties = SchemaAnnotationProcessor.accessResolvedPropertiesByPath("/a/bb", dataSchema);


    System.out.println(pdlStr);
  }



}

//    DataSchemaTraverse dataSchemaTraverse = new SchemaAnnotationTraverse();
//    DataSchemaTraverse dataSchemaTraverse = new DataSchemaTraverse();

//    Map<DataSchemaTraverse.Order, DataSchemaTraverse.Callback> callbacks = new HashMap<>();
//    callbacks.put(DataSchemaTraverse.Order.PRE_ORDER, new SchemaAnnotationProcessor.SchemaAnnotationTraverseCallBack());

//    dataSchemaTraverse.traverse(dataSchema, callbacks);