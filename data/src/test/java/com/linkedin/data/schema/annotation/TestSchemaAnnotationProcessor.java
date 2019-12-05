package com.linkedin.data.schema.annotation;

import com.linkedin.data.TestUtil;
import com.linkedin.data.schema.DataSchema;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.joining;


public class TestSchemaAnnotationProcessor
{
  @DataProvider
  public Object[][] denormalizedSchemaTestCases_invalid()
  {
    return new Object[][]{
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_2_cyclic_simple_overrides_invalid.pdl"},
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid.pdl"},
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid_complex.pdl"},
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_3_cyclic_invalid_deep.pdl"},
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/3_4_cyclic_cross_ref_invalid.pdl"},
        {"com/linkedin/data/schema/annotation/denormalizedsource/invalid/5_pathSpec_invalid.pdl"}};
  }

  @Test(dataProvider = "denormalizedSchemaTestCases_invalid")
  public void testDenormalizedSchemaProcessing_invalid(String filePath) throws Exception
  {
    String moduleDir = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath().toString();
    String fullPath = moduleDir + filePath;

    String pdlStr = new String(Files.readAllBytes(Paths.get(fullPath)));
    DataSchema dataSchema = TestUtil.dataSchemaFromPdlString(pdlStr);

    PegasusSchemaAnnotationHandlerImpl customAnnotationHandler = new PegasusSchemaAnnotationHandlerImpl("customAnnotation");
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult result =
        SchemaAnnotationProcessor.process(Arrays.asList(customAnnotationHandler), dataSchema,
                                          new SchemaAnnotationProcessor.AnnotationProcessOption());
    assert(result.hasError());
  }



  @DataProvider
  public Object[][] denormalizedSchemaTestCases_valid()
  {
    // First element is test file name
    // Second element is array of array, which child array is an array of two elements: <PathSpec> and its annotation
    //   in fact the second element will list all primitive field without recursion.∂
    return new Object[][]{
        {
        // A base case to test primitive type resolvedProperties same as property
        "com/linkedin/data/schema/annotation/denormalizedsource/0_basecase.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=NONE"),
                      Arrays.asList("/a/bb", "customAnnotation=[{data_type=NAME}]"), Arrays.asList("/a/cc", ""),
                      Arrays.asList("/b/aa", "customAnnotation=NONE"),
                      Arrays.asList("/b/bb", "customAnnotation=[{data_type=NAME}]"), Arrays.asList("/b/cc", ""))
        },
        {
            // A base case where has a simple override
            "com/linkedin/data/schema/annotation/denormalizedsource/0_base_recursive_overrides.pdl",
            Arrays.asList(Arrays.asList("/f0/f1/f1/f2", ""),
                          Arrays.asList("/f0/f1/f2", "customAnnotation=sth"),
                          Arrays.asList("/f0/f2", "")),
        },
        {
        // a simple test case on overriding a record being defined
        "com/linkedin/data/schema/annotation/denormalizedsource/0_simpleoverrides.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=[{data_type=NAME}]"),
                      Arrays.asList("/a/bb", "customAnnotation=NONE"), Arrays.asList("/a/cc", ""),
                      Arrays.asList("/b/aa", "customAnnotation=NONE"),
                      Arrays.asList("/b/bb", "customAnnotation=[{data_type=NAME}]"), Arrays.asList("/b/cc", ""))
        },
        {
        // same as above, but this time test overriding the record that already defined.
        "com/linkedin/data/schema/annotation/denormalizedsource/0_simpleoverrides_2.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=NONE"),
                      Arrays.asList("/a/bb", "customAnnotation=[{data_type=NAME}]"), Arrays.asList("/a/cc", ""),
                      Arrays.asList("/b/aa", "customAnnotation=[{data_type=NAME}]"),
                      Arrays.asList("/b/bb", "customAnnotation=NONE"), Arrays.asList("/b/cc", ""))
        },
        {
        // Test case on selectively overriding fields in the record
        "com/linkedin/data/schema/annotation/denormalizedsource/1_0_multiplereference.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=NONE"), Arrays.asList("/a/bb", "customAnnotation=NONE"),
                      Arrays.asList("/b/aa", "customAnnotation=NONE"), Arrays.asList("/b/bb", "customAnnotation=12"),
                      Arrays.asList("/c/aa", "customAnnotation=21"), Arrays.asList("/c/bb", "customAnnotation=NONE"),
                      Arrays.asList("/d/aa", "customAnnotation=NONE"), Arrays.asList("/d/bb", "customAnnotation=NONE"))
        },
        {
        // Test case on selectively overriding fields in the record
        "com/linkedin/data/schema/annotation/denormalizedsource/1_1_testnestedshallowcopy.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=NONE"), Arrays.asList("/a/ab", "customAnnotation=NONE"),
                      Arrays.asList("/b/bb/aa", "customAnnotation=from_field_b"),
                      Arrays.asList("/b/bb/ab", "customAnnotation=NONE"),
                      Arrays.asList("/c/bb/aa", "customAnnotation=from_field_b"),
                      Arrays.asList("/c/bb/ab", "customAnnotation=NONE"),
                      Arrays.asList("/d/bb/aa", "customAnnotation=from_field_d"),
                      Arrays.asList("/d/bb/ab", "customAnnotation=NONE"))
        },
        {
        // Test case on map related field
        "com/linkedin/data/schema/annotation/denormalizedsource/2_1_1_map.pdl",
        Arrays.asList(Arrays.asList("/a/$key", "customAnnotation=[{data_type=NAME}]"),
                      Arrays.asList("/a/*", "customAnnotation=NONE"), Arrays.asList("/b/$key", ""),
                      Arrays.asList("/b/*/bb", ""), Arrays.asList("/c/$key", ""),
                      Arrays.asList("/c/*/bb", "customAnnotation=NONE"), Arrays.asList("/d/$key", "customAnnotation=1st_key"),
                      Arrays.asList("/d/*/$key", "customAnnotation=2nd_key"), Arrays.asList("/d/*/*", "customAnnotation=2nd_value"),
                      Arrays.asList("/e/$key", "customAnnotation=key_value"),
                      Arrays.asList("/e/*/*", "customAnnotation=array_value"),
                      Arrays.asList("/f/$key", "customAnnotation=key_value"),
                      Arrays.asList("/f/*/int", "customAnnotation=union_int_value"),
                      Arrays.asList("/f/*/string", "customAnnotation=union_string_value"),
                      Arrays.asList("/g/map/$key", "customAnnotation=key_value"),
                      Arrays.asList("/g/map/*", "customAnnotation=string_value"),
                      Arrays.asList("/g/int", "customAnnotation=union_int_value"))
        },
        {
        // Test case on array related fields
        "com/linkedin/data/schema/annotation/denormalizedsource/2_1_2_array.pdl",
        Arrays.asList(Arrays.asList("/address/*", "customAnnotation=[{dataType=ADDRESS}]"),
                      Arrays.asList("/address2/*", "customAnnotation=[{dataType=NONE}]"),
                      Arrays.asList("/name/*/*", "customAnnotation=[{dataType=ADDRESS}]"),
                      Arrays.asList("/name2/*/*", "customAnnotation=[{dataType=NONE}]"),
                      Arrays.asList("/nickname/*/int", "customAnnotation=[{dataType=NAME}]"),
                      Arrays.asList("/nickname/*/string", "customAnnotation=[{dataType=NAME}]"))
        },
        {
        // Test case on union related fields
        "com/linkedin/data/schema/annotation/denormalizedsource/2_1_3_union.pdl",
        Arrays.asList(Arrays.asList("/unionField/int", "customAnnotation=NONE"),
                      Arrays.asList("/unionField/string", "customAnnotation=[{dataType=MEMBER_ID, format=URN}]"),
                      Arrays.asList("/unionField/array/*", "customAnnotation={dataType=MEMBER_ID, format=URN}"),
                      Arrays.asList("/unionField/map/$key", "customAnnotation=[{dataType=MEMBER_ID, format=URN}]"),
                      Arrays.asList("/unionField/map/*", "customAnnotation=[{dataType=MEMBER_ID, format=URN}]"),
                      Arrays.asList("/answerFormat/multipleChoice", "customAnnotation=for multipleChoice"),
                      Arrays.asList("/answerFormat/shortAnswer", "customAnnotation=for shortAnswer"),
                      Arrays.asList("/answerFormat/longAnswer", "customAnnotation=for longAnswer"))
        },
        {
            //Test of fixed data schema
            "com/linkedin/data/schema/annotation/denormalizedsource/2_2_1_fixed.pdl",
            Arrays.asList(Arrays.asList("/a", "customAnnotation=NONE"),
                          Arrays.asList("/b/bb", "customAnnotation=b:bb"),
                          Arrays.asList("/c/bb", "customAnnotation=c:bb"),
                          Arrays.asList("/d", "customAnnotation=INNER"))
        },
        {
            //Test of enum
            "com/linkedin/data/schema/annotation/denormalizedsource/2_2_2_enum.pdl",
            Arrays.asList(Arrays.asList("/fruit","customAnnotation=fruit1"),
                          Arrays.asList("/otherFruits","customAnnotation=fruit2"))
        },
        {
            //Test of TypeRefs
            "com/linkedin/data/schema/annotation/denormalizedsource/2_2_3_typeref.pdl",
            Arrays.asList(Arrays.asList("/primitive_field", "customAnnotation=TYPEREF1"),
                          Arrays.asList("/primitive_field2", "customAnnotation=TYPEREF3"),
                          Arrays.asList("/primitive_field3", "customAnnotation=TYPEREF4"),
                          Arrays.asList("/primitive_field4", "customAnnotation=TYPEREF5"),
                          Arrays.asList("/a/$key", ""),
                          Arrays.asList("/a/*/a", "customAnnotation=TYPEREF1"),
                          Arrays.asList("/b/a", "customAnnotation=original_nested"),
                          Arrays.asList("/c/a", "customAnnotation=b: overriden_nested in c"),
                          Arrays.asList("/d", "customAnnotation=TYPEREF1"),
                          Arrays.asList("/e", "customAnnotation=TYPEREF2"))
        },
        {
            //Test of includes
            "com/linkedin/data/schema/annotation/denormalizedsource/2_2_4_includes.pdl",
            Arrays.asList(Arrays.asList("/a/aa","customAnnotation=/a/aa"),
                          Arrays.asList("/a/bb","customAnnotation=/bb"),
                          Arrays.asList("/b","customAnnotation=NONE"),
                          Arrays.asList("/c/ca","customAnnotation=includedRcd2"),
                          Arrays.asList("/c/cb","customAnnotation=upper"),
                          Arrays.asList("/c/cc","customAnnotation=NONE"))
        },
        {
        // simple example case for cyclic reference
        "com/linkedin/data/schema/annotation/denormalizedsource/3_1_cyclic_simple_valid.pdl",
        Arrays.asList(Arrays.asList("/name", "customAnnotation=none"))
        },
        {
        // example of valid usage of cyclic schema referencing: referencing a recursive structure, from outside
            "com/linkedin/data/schema/annotation/denormalizedsource/3_2_cyclic_multiplefields.pdl",
        Arrays.asList(Arrays.asList("/a/aa", "customAnnotation=aa"), Arrays.asList("/b/aa", "customAnnotation=b:/aa"),
                      Arrays.asList("/b/bb/aa", "customAnnotation=b:/bb/aa"),
                      Arrays.asList("/b/bb/bb/aa", "customAnnotation=b:/bb/bb/aa"),
                      Arrays.asList("/b/bb/bb/bb/aa", "customAnnotation=aa"),
                      Arrays.asList("/b/bb/bb/cc/aa", "customAnnotation=aa"), Arrays.asList("/b/bb/cc/aa", "customAnnotation=aa"),
                      Arrays.asList("/b/cc/aa", "customAnnotation=aa"), Arrays.asList("/c/aa", "customAnnotation=c:/aa"),
                      Arrays.asList("/c/bb/aa", "customAnnotation=c:/bb/aa"),
                      Arrays.asList("/c/bb/bb/aa", "customAnnotation=c:/bb/bb/aa"),
                      Arrays.asList("/c/bb/bb/bb/aa", "customAnnotation=aa"),
                      Arrays.asList("/c/bb/bb/cc/aa", "customAnnotation=aa"), Arrays.asList("/c/bb/cc/aa", "customAnnotation=aa"),
                      Arrays.asList("/c/cc/aa", "customAnnotation=aa"))
        },
        {
        // example of valid usage of cyclic schema referencing: referencing a recursive structure, from outside
            "com/linkedin/data/schema/annotation/denormalizedsource/4_1_comprehensive_example.pdl",
        Arrays.asList(Arrays.asList("/memberId", "customAnnotation=[{dataType=MEMBER_ID_INT, isPurgeKey=true}]"),
                      Arrays.asList("/memberData/usedNames/*/*", "customAnnotation=[{dataType=NAME}]"),
                      Arrays.asList("/memberData/phoneNumber", "customAnnotation=[{dataType=PHONE_NUMBER}]"),
                      Arrays.asList("/memberData/address/*", "customAnnotation=[{dataType=ADDRESS}]"),
                      Arrays.asList("/memberData/workingHistory/$key", "customAnnotation=workinghistory-$key"),
                      Arrays.asList("/memberData/workingHistory/*", "customAnnotation=workinghistory-value"),
                      Arrays.asList("/memberData/details/firstName", "customAnnotation=[{dataType=MEMBER_FIRST_NAME}]"),
                      Arrays.asList("/memberData/details/lastName", "customAnnotation=[{dataType=MEMBER_LAST_NAME}]"),
                      Arrays.asList("/memberData/details/otherNames/*/*/nickName", "customAnnotation=[{dataType=MEMBER_LAST_NAME}]"),
                      Arrays.asList("/memberData/details/otherNames/*/*/shortCutName", "customAnnotation=[{dataType=MEMBER_LAST_NAME}]"),
                      Arrays.asList("/memberData/education/string", "customAnnotation=NONE"),
                      Arrays.asList("/memberData/education/array/*/graduate", "customAnnotation=[{dataType=MEMBER_GRADUATION}]"))
        },
        {
            // example of valid usage of cyclic schema referencing: referencing a recursive structure, from outside
            "com/linkedin/data/schema/annotation/denormalizedsource/4_2_multiplepaths_deep_overrides.pdl",
            Arrays.asList(Arrays.asList("/a/a1", "customAnnotation=Level1: a1"),
                          Arrays.asList("/a/a2/aa1/aaa1", "customAnnotation=Level1: /a2/aa1/aaa1"),
                          Arrays.asList("/a/a2/aa1/aaa2", "customAnnotation=Level1: /a2/aa1/aaa2"),
                          Arrays.asList("/a/a2/aa1/aaa3/*", "customAnnotation=Level1: /a2/aa1/aaa3/*"),
                          Arrays.asList("/a/a2/aa1/aaa4/*/*", "customAnnotation=Level1: /a2/aa1/aaa4/*/*"),
                          Arrays.asList("/a/a2/aa1/aaa5/$key", "customAnnotation=Level1: /a2/aa1/aaa5/$key"),
                          Arrays.asList("/a/a2/aa1/aaa5/*", "customAnnotation=Level1: /a2/aa1/aaa5/*"),
                          Arrays.asList("/a/a2/aa1/aaa6/$key", "customAnnotation=Level1: /a2/aa1/aaa6/$key"),
                          Arrays.asList("/a/a2/aa1/aaa6/*/*", "customAnnotation=Level1: /a2/aa1/aaa6/*/*"),
                          Arrays.asList("/a/a2/aa1/aaa7/array/*", "customAnnotation=Level1: /a2/aa1/aaa7/array/*"),
                          Arrays.asList("/a/a2/aa1/aaa7/int", "customAnnotation=Level1: /a2/aa1/aaa7/int"),
                          Arrays.asList("/a/a2/aa1/aaa8/map/$key", "customAnnotation=Level1: /a2/aa1/aaa8/map/$key"),
                          Arrays.asList("/a/a2/aa1/aaa8/map/*", "customAnnotation=Level1: /a2/aa1/aaa8/map/*"),
                          Arrays.asList("/a/a2/aa1/aaa8/int", "customAnnotation=Level1: /a2/aa1/aaa8/int"),
                          Arrays.asList("/a/a3/bb1", "customAnnotation=Level1: /a3/bb1"),
                          Arrays.asList("/a/a3/bb2", "customAnnotation=Level1: /a3/bb2"))
        }
    };
  }

  @Test(dataProvider = "denormalizedSchemaTestCases_valid")
  public void testDenormalizedSchemaProcessing(String filePath, List<List<String>> expected) throws Exception
  {
    String moduleDir = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath().toString();
    String fullPath = moduleDir + filePath;

    String pdlStr = new String(Files.readAllBytes(Paths.get(fullPath)));
    DataSchema dataSchema = TestUtil.dataSchemaFromPdlString(pdlStr);

    PegasusSchemaAnnotationHandlerImpl customAnnotationHandler = new PegasusSchemaAnnotationHandlerImpl("customAnnotation");
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult result =
        SchemaAnnotationProcessor.process(Arrays.asList(customAnnotationHandler), dataSchema,
                                          new SchemaAnnotationProcessor.AnnotationProcessOption());

    ResolvedPropertiesReaderVisitor resolvedPropertiesReaderVisitor = new ResolvedPropertiesReaderVisitor();
    DataSchemaRichContextTraverser traverser = new DataSchemaRichContextTraverser(resolvedPropertiesReaderVisitor);
    traverser.traverse(result.getResultSchema());
    Map<String, Map<String, Object>> pathSpecToResolvedPropertiesMap = resolvedPropertiesReaderVisitor.getLeafFieldsPathSpecToResolvedPropertiesMap();
    Assert.assertEquals(pathSpecToResolvedPropertiesMap.entrySet().size(), expected.size());

    for (List<String> pair : expected)
    {
      String pathSpec = pair.get(0);
      String expectedProperties = pair.get(1);
      Map<String, Object> resolvedProperties =
          SchemaAnnotationProcessor.getResolvedPropertiesByPath(pathSpec, result.getResultSchema());
      String resolvedPropertiesStr =
          resolvedProperties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining("&"));
      assert (expectedProperties.equals(resolvedPropertiesStr));
    }
  }
}

