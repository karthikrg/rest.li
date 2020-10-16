/*
   Copyright (c) 2020 LinkedIn Corp.

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

package com.linkedin.data.collections;

import com.linkedin.data.Data;
import com.linkedin.data.DataComplex;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map implementation optimized for a particular {@link com.linkedin.data.template.RecordTemplate} or
 * {@link com.linkedin.data.template.UnionTemplate} schema.
 */
public abstract class SpecificDataTemplateSchemaMap extends SpecificMap
{
  /**
   * Initial capacity of the extra fields map.
   */
  private static final int EXTRA_FIELDS_INITIAL_CAPACITY = 4;

  /**
   * A placeholder object to indicate that this is an extra field that is unknown to the schema.
   */
  protected static final Object EXTRA_FIELD = new Object();

  /**
   * A map containing extra fields that are not defined in the schema. Lazily initialized.
   */
  private HashMap<String, Object> _extraFieldsMap;

  @Override
  public int size()
  {
    return specificSize() + (_extraFieldsMap == null ? 0 : _extraFieldsMap.size());
  }

  protected abstract int specificSize();

  @Override
  public boolean isEmpty()
  {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key)
  {
    if (!(key instanceof String))
    {
      return false;
    }

    if (specificGet((String) key) != null)
    {
      return true;
    }

    if (_extraFieldsMap != null)
    {
      return _extraFieldsMap.containsKey(key);
    }

    return false;
  }

  @Override
  public boolean containsValue(Object value)
  {
    if (specificContainsValue(value))
    {
      return true;
    }

    if (_extraFieldsMap != null)
    {
      return _extraFieldsMap.containsValue(value);
    }

    return false;
  }

  protected abstract boolean specificContainsValue(Object value);

  @Override
  public Object get(Object key)
  {
    if (!(key instanceof String))
    {
      return null;
    }

    Object value = specificGet((String) key);
    if (value == null && _extraFieldsMap != null)
    {
      return _extraFieldsMap.get(key);
    }

    return value;
  }

  protected abstract Object specificGet(String key);

  @Override
  public Object put(String key, Object value)
  {
    if (value == null)
    {
      return remove(key);
    }

    Object oldValue = specificPut(key, value);
    if (oldValue == EXTRA_FIELD)
    {
      if (_extraFieldsMap == null)
      {
        _extraFieldsMap = new HashMap<>(EXTRA_FIELDS_INITIAL_CAPACITY);
      }

      return _extraFieldsMap.put(key, value);
    }

    return oldValue;
  }

  protected abstract Object specificPut(String key, Object value);

  @Override
  public Object remove(Object key)
  {
    if (!(key instanceof String))
    {
      return null;
    }

    Object removed = specificRemove((String) key);
    if (removed == EXTRA_FIELD && _extraFieldsMap != null)
    {
      return _extraFieldsMap.remove(key);
    }

    return removed;
  }

  protected abstract Object specificRemove(String key);

  @Override
  public void clear()
  {
    specificClear();
    if (_extraFieldsMap != null)
    {
      _extraFieldsMap.clear();
    }
  }

  protected abstract void specificClear();

  @Override
  public Set<String> keySet()
  {
    return new KeySet(this);
  }

  @Override
  public Collection<Object> values()
  {
    return new ValueCollection(this);
  }

  @Override
  public Set<Entry<String, Object>> entrySet()
  {
    return new EntrySet(this);
  }

  protected abstract Entry<String, Object> specificNextEntry(Entry<String, Object> current);

  @Override
  @SuppressWarnings("unchecked")
  public void forEach(BiConsumer<? super String, ? super Object> action)
  {
    // We intentionally downcast here since JCodeModel which is used for code generation doesn't support
    // lower bound wildcards.
    specificForEach((BiConsumer<String, Object>) action);
    if (_extraFieldsMap != null)
    {
      _extraFieldsMap.forEach(action);
    }
  }

  protected abstract void specificForEach(BiConsumer<String, Object> action);

  @Override
  public void traverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException
  {
    specificTraverse(callback, cycleChecker);

    if (_extraFieldsMap == null || _extraFieldsMap.isEmpty())
    {
      return;
    }

    try
    {
      _extraFieldsMap.forEach((key, value) ->
      {
        try
        {
          callback.key(key);
          Data.traverse(value, callback, cycleChecker);
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

  protected abstract void specificTraverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException;

  /**
   * Returns a shallow copy of this instance: the keys and values themselves are not cloned.
   *
   * @return a shallow copy of this map
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object clone() throws CloneNotSupportedException
  {
    SpecificDataTemplateSchemaMap clone = (SpecificDataTemplateSchemaMap) super.clone();
    if (_extraFieldsMap != null)
    {
      clone._extraFieldsMap = (HashMap<String, Object>) _extraFieldsMap.clone();
    }
    else
    {
      clone._extraFieldsMap = null;
    }

    return clone;
  }

  /**
   * Deep copy.
   *
   * Clones this object, deep copies complex Data objects referenced by this object, and
   * update internal references to point to the deep copies.
   *
   * @throws CloneNotSupportedException if the object cannot be deep copied.
   */
  @Override
  public SpecificMap copy() throws CloneNotSupportedException
  {
    SpecificDataTemplateSchemaMap copy = specificCopy();
    copy._extraFieldsMap = copyMap(_extraFieldsMap);

    return copy;
  }

  protected abstract SpecificDataTemplateSchemaMap specificCopy() throws CloneNotSupportedException;

  private static class KeySet extends AbstractSet<String>
  {
    private final SpecificDataTemplateSchemaMap _parent;

    KeySet(SpecificDataTemplateSchemaMap parent)
    {
      _parent = parent;
    }

    @Override
    public int size()
    {
      return _parent.size();
    }

    public void clear()
    {
      _parent.clear();
    }

    public boolean contains(Object key)
    {
      return _parent.containsKey(key);
    }

    public boolean remove(Object key)
    {
      boolean result = _parent.containsKey(key);
      _parent.remove(key);
      return result;
    }

    public Iterator<String> iterator()
    {
      return _parent.isEmpty() ? Collections.emptyIterator() : new KeySetIterator(_parent);
    }
  }

  private static class ValueCollection extends AbstractCollection<Object>
  {
    private final SpecificDataTemplateSchemaMap _parent;

    ValueCollection(SpecificDataTemplateSchemaMap parent)
    {
      _parent = parent;
    }

    public int size()
    {
      return _parent.size();
    }

    public void clear()
    {
      _parent.clear();
    }

    public boolean contains(Object value)
    {
      return _parent.containsValue(value);
    }

    public Iterator<Object> iterator()
    {
      return _parent.isEmpty() ? Collections.emptyIterator() : new ValuesIterator(_parent);
    }
  }

  static class EntrySet extends AbstractSet<Entry<String, Object>>
  {
    private final SpecificDataTemplateSchemaMap _parent;

    EntrySet(SpecificDataTemplateSchemaMap parent)
    {
      _parent = parent;
    }

    public int size()
    {
      return _parent.size();
    }

    public void clear()
    {
      _parent.clear();
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object obj)
    {
      if (!(obj instanceof Entry))
      {
        return false;
      }
      else
      {
        Entry<String, Object> entry = (Entry<String, Object>)obj;
        String key = entry.getKey();
        boolean result = _parent.containsKey(key);
        _parent.remove(key);
        return result;
      }
    }

    public Iterator<Entry<String, Object>> iterator()
    {
      return _parent.isEmpty() ? Collections.emptyIterator() : new EntrySetIterator(_parent);
    }
  }

  static class EntrySetIterator implements Iterator<Entry<String, Object>>
  {
    private final SpecificDataTemplateSchemaMap _parent;
    private Iterator<Entry<String, Object>> _extraFieldsIterator;
    private int _specificElementIndex = 0;
    private int _specificSize = 0;
    private Entry<String, Object> _current = null;
    private Entry<String, Object> _previous = null;

    EntrySetIterator(SpecificDataTemplateSchemaMap parent)
    {
      _parent = parent;
      _extraFieldsIterator = _parent._extraFieldsMap != null ?
          _parent._extraFieldsMap.entrySet().iterator() : Collections.emptyIterator();
      _specificSize = parent.specificSize();
    }

    public boolean hasNext()
    {
      return _specificElementIndex < _specificSize || _extraFieldsIterator.hasNext();
    }

    public Entry<String, Object> next()
    {
      if (!hasNext())
      {
        throw new NoSuchElementException("No next() entry in the iteration");
      }
      else
      {
        if (_specificElementIndex < _specificSize)
        {
          _current = _parent.specificNextEntry(_previous);
          _previous = _current;
          _specificElementIndex++;
        }
        else
        {
          _current = _extraFieldsIterator.next();
        }

        return _current;
      }
    }

    public void remove()
    {
      if (_current == null)
      {
        throw new IllegalStateException("remove() can only be called once after next()");
      }
      else
      {
        Object removed = _parent.specificRemove(_current.getKey());

        if (removed != null && removed != EXTRA_FIELD)
        {
          _specificElementIndex--;
          _specificSize--;
        }

        if (removed == EXTRA_FIELD && _parent._extraFieldsMap != null)
        {
          _parent._extraFieldsMap.remove(_current.getKey());
        }

        _previous = _current;
        _current = null;
      }
    }
  }

  static class KeySetIterator implements Iterator<String>
  {
    private final EntrySetIterator _entrySetIterator;

    KeySetIterator(SpecificDataTemplateSchemaMap parent)
    {
      _entrySetIterator = new EntrySetIterator(parent);
    }

    @Override
    public boolean hasNext()
    {
      return _entrySetIterator.hasNext();
    }

    public String next()
    {
      Entry<String, Object> entry = _entrySetIterator.next();
      if (entry == null)
      {
        throw new IllegalStateException("No next() entry in the iteration");
      }
      return entry.getKey();
    }

    @Override
    public void remove()
    {
      _entrySetIterator.remove();
    }
  }

  static class ValuesIterator implements Iterator<Object>
  {
    private final EntrySetIterator _entrySetIterator;

    ValuesIterator(SpecificDataTemplateSchemaMap parent)
    {
      _entrySetIterator = new EntrySetIterator(parent);
    }

    @Override
    public boolean hasNext()
    {
      return _entrySetIterator.hasNext();
    }

    public Object next()
    {
      Entry<String, Object> entry = _entrySetIterator.next();
      if (entry == null)
      {
        throw new IllegalStateException("No next() entry in the iteration");
      }
      return entry.getValue();
    }

    @Override
    public void remove()
    {
      _entrySetIterator.remove();
    }
  }

  public static class SpecificMapEntry extends AbstractMap.SimpleEntry<String, Object>
  {
    private static final long serialVersionUID = -8499721149061103595L;

    private final SpecificMap _parent;

    public SpecificMapEntry(String key, Object value, SpecificMap parent)
    {
      super(key, value);
      _parent = parent;
    }

    @Override
    public Object setValue(Object value)
    {
      Object oldValue = super.setValue(value);
      _parent.put(getKey(), value);
      return oldValue;
    }
  }
}
