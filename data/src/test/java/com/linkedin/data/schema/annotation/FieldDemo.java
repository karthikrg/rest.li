package com.linkedin.data.schema.annotation;

import com.linkedin.data.DataMap;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;


public class FieldDemo {

  public static void main(String[] args) throws NoSuchFieldException,
                                                SecurityException, IllegalArgumentException, IllegalAccessException {

//    Map<String, List<String>> queryParameters = UriComponent.decodeQuery(_request.getURI(), false);
//    DataMap _parameters = URIParamUtils.parseUriParams(queryParameters);
//    SampleClass sampleObject = new SampleClass();
//    sampleObject.setSampleField("data");
//
//    Field field = SampleClass.class.getField("testField");
//    NestedClass gotObject = (NestedClass) field.get(sampleObject);
//    NestedClass gotObject2 = (NestedClass) field.get("null");
////    System.out.println(field.get(sampleObject));
//    System.out.println(gotObject.value2);
  }
}

@CustomAnnotation(name = "SampleClass",  value = "Sample Class Annotation")
class SampleClass {

  @CustomAnnotation(name="sampleClassField",  value = "Sample Field Annotation")
  public String sampleField;

  public String getSampleField() {
    return sampleField;
  }

  public void setSampleField(String sampleField) {
    this.sampleField = sampleField;
  }

  public static final NestedClass testField = new NestedClass("hehe");

}

class NestedClass{
  public void setValue2(String value2)
  {
    this.value2 = value2;
  }

  String value2;

  public NestedClass(String value2)
  {
    this.value2 = value2;
  }
}

@Retention(RetentionPolicy.RUNTIME)
@interface CustomAnnotation {
  public String name();
  public String value();
}
