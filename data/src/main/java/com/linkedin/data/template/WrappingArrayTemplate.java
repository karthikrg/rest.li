/*
   Copyright (c) 2012 LinkedIn Corp.

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

package com.linkedin.data.template;

import com.linkedin.data.Data;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.collections.CheckedUtil;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.util.ArgumentUtil;
import com.linkedin.util.Lazy;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;


/**
 * Abstract class for array of value types that require proxying by a {@link DataTemplate}.
 *
 * @param <E> is the element type of the array.
 */
public class WrappingArrayTemplate<E extends DataTemplate<?>> extends AbstractArrayTemplate<E>
{
  /**
   * Constructor.
   *
   * @param list is the underlying {@link DataList} that will be proxied by this {@link WrappingArrayTemplate}.
   * @param schema is the {@link DataSchema} of the array.
   * @param elementClass is the class of elements returned by this {@link WrappingArrayTemplate}.
   */
  protected WrappingArrayTemplate(DataList list, ArrayDataSchema schema, Class<E> elementClass)
      throws TemplateOutputCastException
  {
    this(list, schema, elementClass, DataTemplateUtil.getDataClass(schema.getItems()));
  }

  /**
   * Constructor.
   *
   * @param list is the underlying {@link DataList} that will be proxied by this {@link WrappingArrayTemplate}.
   * @param schema is the {@link DataSchema} of the array.
   * @param elementClass is the class of elements returned by this {@link WrappingArrayTemplate}.
   * @param elementDataClass is the class of raw data of elements returned by this {@link WrappingArrayTemplate}.
   */
  protected WrappingArrayTemplate(DataList list, ArrayDataSchema schema, Class<E> elementClass, Class<?> elementDataClass)
      throws TemplateOutputCastException
  {
    super(list, schema, elementClass, elementDataClass);
    final int size = list.size();
    _cache = new Lazy<>(() -> new DataListCache<>(size));
  }

  @Override
  public boolean add(E element) throws ClassCastException
  {
    boolean result = CheckedUtil.addWithoutChecking(_list, unwrap(element));
    modCount++;
    return result;
  }

  @Override
  public void add(int index, E element) throws ClassCastException
  {
    CheckedUtil.addWithoutChecking(_list, index, unwrap(element));
    modCount++;
  }

  @Override
  public E get(int index) throws TemplateOutputCastException
  {
    return cacheLookup(_list.get(index), index, false);
  }

  @Override
  public E remove(int index) throws TemplateOutputCastException
  {
    Object removed = _list.remove(index);
    modCount++;
    return cacheLookup(removed, index, true);
  }

  @Override
  public void removeRange(int fromIndex, int toIndex)
  {
    _list.removeRange(fromIndex, toIndex);
    modCount++;
  }

  @Override
  public E set(int index, E element) throws ClassCastException, TemplateOutputCastException
  {
    Object replaced = CheckedUtil.setWithoutChecking(_list, index, unwrap(element));
    modCount++;
    return cacheLookup(replaced, index, true);
  }

  @Override
  public WrappingArrayTemplate<E> clone() throws CloneNotSupportedException
  {
    WrappingArrayTemplate<E> clone = (WrappingArrayTemplate<E>) super.clone();
    clone._cache = new Lazy<>(clone._cache.get().clone());
    return clone;
  }

  @Override
  public WrappingArrayTemplate<E> copy() throws CloneNotSupportedException
  {
    WrappingArrayTemplate<E> copy = (WrappingArrayTemplate<E>) super.copy();
    copy._cache = new Lazy<>(() -> new DataListCache<>(size()));
    return copy;
  }

  /**
   * Obtain the underlying Data object of the {@link DataTemplate} object.
   *
   * This method checks that the provided object's class is
   * the element class of the {@link WrappingArrayTemplate}.
   *
   * @param object provides the input {@link DataTemplate} object.
   * @return the underlying Data object.
   * @throws ClassCastException if the object's class is not the
   *                            element class of the {@link WrappingArrayTemplate}.
   */
  protected Object unwrap(E object) throws ClassCastException
  {
    ArgumentUtil.notNull(object, "object");
    if (object.getClass() == _elementClass)
    {
      return object.data();
    }
    else
    {
      throw new ClassCastException("Input " + object + " should be a " + _elementClass.getName());
    }
  }

  /**
   * Lookup the {@link DataTemplate} for a Data object, if not cached,
   * create a {@link DataTemplate} for the Data object and add it to the cache.
   *
   * @param object is the Data object.
   * @param index of the Data object in the underlying {@link DataList}.
   * @param removeExisting remove the element at the existing index from the backing cache.
   *
   * @return the {@link DataTemplate} that proxies the Data object.
   * @throws TemplateOutputCastException if the object cannot be wrapped.
   */
  protected E cacheLookup(Object object, int index, boolean removeExisting) throws TemplateOutputCastException
  {
    E wrapped;
    assert(object != null);
    DataListCache<E> cache = _cache.get();
    if ((wrapped = cache.get(index)) == null || wrapped.data() != object)
    {
      wrapped = coerceOutput(object);
      cache.put(index, (removeExisting ? null : wrapped));
    }
    return wrapped;
  }

  protected E coerceOutput(Object value) throws TemplateOutputCastException
  {
    if (_constructor == null)
    {
      _constructor = DataTemplateUtil.templateConstructor(_elementClass, schema().getItems());
    }

    return DataTemplateUtil.wrap(value, _constructor);
  }

  private Constructor<E> _constructor;
  protected Lazy<DataListCache<E>> _cache;

  public static class DataMapSpecificElementArray extends SpecificElementArrayTemplate<DataMap>
  {
    public DataMapSpecificElementArray()
    {
      super(DataMap.class);
    }

    public DataMapSpecificElementArray(int capacity)
    {
      super(capacity, DataMap.class);
    }

    @Override
    protected void specificTraverse(DataMap object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      object.traverse(callback, cycleChecker);
    }
  }

  public static class DataListSpecificElementArray extends SpecificElementArrayTemplate<DataList>
  {
    public DataListSpecificElementArray()
    {
      super(DataList.class);
    }

    public DataListSpecificElementArray(int capacity)
    {
      super(capacity, DataList.class);
    }

    @Override
    protected void specificTraverse(DataList object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker)
        throws IOException
    {
      object.traverse(callback, cycleChecker);
    }
  }

  private static class DataListCache<E>
  {
    private E[] _list;

    @SuppressWarnings("unchecked")
    public DataListCache(int size)
    {
      _list = (E[]) new Object[size];
    }

    private DataListCache(E[] list)
    {
      _list = list;
    }

    public E get(int index)
    {
      if (index < 0 || index >= _list.length)
      {
        return null;
      }

      return _list[index];
    }

    @SuppressWarnings("unchecked")
    public void put(int index, E value)
    {
      if (index >= _list.length)
      {
        // Grow generously to avoid frequent resizing.
        E[] newList = (E[]) new Object[Math.max(index * 2, 2)];
        System.arraycopy(_list, 0, newList, 0, _list.length);
        _list = newList;
      }
      _list[index] = value;
    }

    public DataListCache<E> clone() throws CloneNotSupportedException
    {
      return new DataListCache<>(Arrays.copyOf(_list, _list.length));
    }
  }
}

