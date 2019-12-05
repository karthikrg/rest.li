package com.linkedin.data.schema.annotation;

import com.linkedin.data.TestUtil;
import com.linkedin.data.schema.DataSchema;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.joining;


public class TestSchemaAnnotationProcessorTraverserVersion
{

  @DataProvider
  public Object[][] denormalizedSchemaTestCases()
  {
    // First element is test file name
    // Second element is array of array, which child array is an array of two elements: <PathSpec> and its annotation
    //   in fact the second element will list all primitive field without recursion.∂
    return new Object[][]{
        {
          // A base case to test primitive type resolvedProperties same as property
          "com/linkedin/data/schema/annotation/denormalizedsource/0_basecase.pdl",
            Arrays.asList(Arrays.asList("/a/aa", "compliance=NONE"),
                          Arrays.asList("/a/bb", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/a/cc", ""),
                          Arrays.asList("/b/aa", "compliance=NONE"),
                          Arrays.asList("/b/bb", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/b/cc", ""))
        },
        {
          // a simple test case on overriding a record being defined
            "com/linkedin/data/schema/annotation/denormalizedsource/0_simpleoverrides.pdl",
            Arrays.asList(Arrays.asList("/a/aa", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/a/bb", "compliance=NONE"),
                          Arrays.asList("/a/cc", ""),
                          Arrays.asList("/b/aa", "compliance=NONE"),
                          Arrays.asList("/b/bb", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/b/cc", ""))
        },
        {
            // same as above, but this time test overriding the record that already defined.
            "com/linkedin/data/schema/annotation/denormalizedsource/0_simpleoverrides_2.pdl",
            Arrays.asList(Arrays.asList("/a/aa", "compliance=NONE"),
                          Arrays.asList("/a/bb", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/a/cc", ""),
                          Arrays.asList("/b/aa", "compliance=[{data_type=NAME}]"),
                          Arrays.asList("/b/bb", "compliance=NONE"),
                          Arrays.asList("/b/cc", ""))
        },
        {
            // Test case on selectively overriding fields in the record
            "com/linkedin/data/schema/annotation/denormalizedsource/1_0_multiplereference.pdl",
            Arrays.asList(Arrays.asList("/a/aa", "compliance=NONE"),
                          Arrays.asList("/a/bb", "compliance=NONE"),
                          Arrays.asList("/b/aa", "compliance=NONE"),
                          Arrays.asList("/b/bb", "compliance=12"),
                          Arrays.asList("/c/aa", "compliance=21"),
                          Arrays.asList("/c/bb", "compliance=NONE"),
                          Arrays.asList("/d/aa", "compliance=NONE"),
                          Arrays.asList("/d/bb", "compliance=NONE"))
        },
        {
            // Test case on selectively overriding fields in the record
            "com/linkedin/data/schema/annotation/denormalizedsource/1_1_testnestedshallowcopy.pdl",
            Arrays.asList(Arrays.asList("/a/aa", "compliance=NONE"),
                          Arrays.asList("/a/ab", "compliance=NONE"),
                          Arrays.asList("/b/bb/aa", "compliance=from_field_b"),
                          Arrays.asList("/b/bb/ab", "compliance=NONE"),
                          Arrays.asList("/c/bb/aa", "compliance=from_field_b"),
                          Arrays.asList("/c/bb/ab", "compliance=NONE"),
                          Arrays.asList("/d/bb/aa", "compliance=from_field_d"),
                          Arrays.asList("/d/bb/ab", "compliance=NONE"))
        }
    };
  }

  @Test(dataProvider = "denormalizedSchemaTestCases")
  public void testDenormalizedSchemaProcessing(String filePath, List<List<String>> expected) throws Exception
  {
    String moduleDir = getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath().toString();
    String fullPath = moduleDir + filePath;

    String pdlStr = new String(Files.readAllBytes(Paths.get(fullPath)));
    DataSchema dataSchema = TestUtil.dataSchemaFromPdlString(pdlStr);

    //Replace with AnnotationProcessor
    SchemaAnnotationTraverser traverser = new SchemaAnnotationTraverser();
    traverser.traverse(dataSchema);


//    //Replace with AnnotationProcessor
//    SchemaAnnotationTraverser traverser = new SchemaAnnotationTraverser();
//    traverser.traverse(dataSchema);

    for (List<String> pair : expected)
    {
      String pathSpec = pair.get(0);
      String expectedProperties = pair.get(1);
      Map<String, Object> resolvedProperties =
          SchemaAnnotationProcessor.getResolvedPropertiesByPath(pathSpec, dataSchema);
      String resolvedPropertiesStr =
          resolvedProperties.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining("&"));
      assert (expectedProperties.equals(resolvedPropertiesStr));
    }
  }
}

