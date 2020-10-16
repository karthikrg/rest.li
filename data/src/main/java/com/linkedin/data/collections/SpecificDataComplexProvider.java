package com.linkedin.data.collections;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public interface SpecificDataComplexProvider
{
  SpecificDataComplexProvider DEFAULT = new SpecificDataComplexProvider(){};

  default Map<String, Object> getMap()
  {
    return new HashMap<>();
  }

  default Map<String, Object> getMap(int capacity)
  {
    return new HashMap<>(capacity);
  }

  default List<Object> getList()
  {
    return new ArrayList<>();
  }

  default List<Object> getList(int capacity)
  {
    return new ArrayList<>(capacity);
  }

  default SpecificDataComplexProvider getChild(String key)
  {
    return SpecificDataComplexProvider.DEFAULT;
  }

  default SpecificDataComplexProvider getChild()
  {
    return SpecificDataComplexProvider.DEFAULT;
  }
}
