package com.linkedin.data.template;

import com.linkedin.data.Data;
import com.linkedin.data.collections.SpecificMap;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map implementation optimized for cases where all the map values are expected to be of a fixed type.
 */
public abstract class SpecificValueMapTemplate<V> extends SpecificMap
{
  private HashMap<String, Object> _embeddedMap;
  private final Class<V> _expectedValueClass;

  protected SpecificValueMapTemplate(Class<V> expectedValueClass)
  {
    _embeddedMap = new HashMap<>();
    _expectedValueClass = expectedValueClass;
  }

  protected SpecificValueMapTemplate(int capacity, Class<V> expectedValueClass)
  {
    _embeddedMap = new HashMap<>(capacity);
    _expectedValueClass = expectedValueClass;
  }

  protected SpecificValueMapTemplate(int capacity, float loadFactor, Class<V> expectedValueClass)
  {
    _embeddedMap = new HashMap<>(capacity, loadFactor);
    _expectedValueClass = expectedValueClass;
  }

  @Override
  public Object get(Object key)
  {
    return _embeddedMap.get(key);
  }

  @Override
  public Object put(String key, Object value)
  {
    return _embeddedMap.put(key, value);
  }

  @Override
  public Object remove(Object key)
  {
    return _embeddedMap.remove(key);
  }

  @Override
  public void clear()
  {
    _embeddedMap.clear();
  }

  @Override
  public void traverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException
  {
    try
    {
      _embeddedMap.forEach((key, value) -> {
        try
        {
          callback.key(key);

          if (value == null)
          {
            callback.nullValue();
            return;
          }

          V castObject;
          try
          {
            castObject = _expectedValueClass.cast(value);
          }
          catch (ClassCastException e)
          {
            Data.traverse(value, callback, cycleChecker);
            return;
          }

          specificTraverse(castObject, callback, cycleChecker);
        }
        catch (IOException e)
        {
          throw new IllegalStateException(e);
        }
      });
    }
    catch (IllegalStateException e)
    {
      if (e.getCause() instanceof IOException)
      {
        throw (IOException) e.getCause();
      }
      else
      {
        throw new IOException(e);
      }
    }
  }

  protected abstract void specificTraverse(V object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException;

  @SuppressWarnings("unchecked")
  @Override
  public Object clone() throws CloneNotSupportedException
  {
    SpecificValueMapTemplate<V> clone = (SpecificValueMapTemplate<V>) super.clone();
    clone._embeddedMap = (HashMap<String, Object>) _embeddedMap.clone();
    return clone;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SpecificMap copy() throws CloneNotSupportedException
  {
    SpecificValueMapTemplate<V> copy = (SpecificValueMapTemplate<V>) super.clone();
    copy._embeddedMap = copyMap(_embeddedMap);
    return copy;
  }

  @Override
  public int size()
  {
    return _embeddedMap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return _embeddedMap.isEmpty();
  }

  @Override
  public Set<String> keySet()
  {
    return _embeddedMap.keySet();
  }

  @Override
  public Collection<Object> values()
  {
    return _embeddedMap.values();
  }

  @Override
  public Set<Entry<String, Object>> entrySet()
  {
    return _embeddedMap.entrySet();
  }

  @Override
  public boolean containsKey(Object key)
  {
    return _embeddedMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value)
  {
    return _embeddedMap.containsValue(value);
  }

  @Override
  public void forEach(BiConsumer<? super String, ? super Object> action)
  {
    _embeddedMap.forEach(action);
  }
}
