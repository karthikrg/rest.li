package com.linkedin.data.template;

import com.linkedin.data.Data;
import com.linkedin.data.collections.SpecificList;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.function.Consumer;

/**
 * A list implementation optimized for cases where all the list elements are expected to be of a fixed type.
 */
public abstract class SpecificElementArrayTemplate<E> extends SpecificList
{
  private ArrayList<Object> _embeddedList;
  private final Class<E> _expectedElementClass;

  protected SpecificElementArrayTemplate(Class<E> expectedElementClass)
  {
    _embeddedList = new ArrayList<>();
    _expectedElementClass = expectedElementClass;
  }

  protected SpecificElementArrayTemplate(int capacity, Class<E> expectedElementClass)
  {
    _embeddedList = new ArrayList<>(capacity);
    _expectedElementClass = expectedElementClass;
  }

  @Override
  public boolean add(Object o)
  {
    return _embeddedList.add(o);
  }

  @Override
  public void add(int index, Object element)
  {
    _embeddedList.add(index, element);
  }

  @Override
  public Object set(int index, Object element)
  {
    return _embeddedList.set(index, element);
  }

  @Override
  public Object get(int index)
  {
    return _embeddedList.get(index);
  }

  @Override
  public int indexOf(Object o)
  {
    return _embeddedList.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o)
  {
    return _embeddedList.lastIndexOf(o);
  }

  @Override
  public Object remove(int index)
  {
    return _embeddedList.remove(index);
  }

  @Override
  public void clear()
  {
    _embeddedList.clear();
  }

  @Override
  public void traverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException
  {
    try
    {
      _embeddedList.forEach(value -> {
        try
        {
          if (value == null)
          {
            callback.nullValue();
            return;
          }

          E castObject;
          try
          {
            castObject = _expectedElementClass.cast(value);
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

  protected abstract void specificTraverse(E object, Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException;

  @SuppressWarnings("unchecked")
  @Override
  public Object clone() throws CloneNotSupportedException
  {
    SpecificElementArrayTemplate<E> clone = (SpecificElementArrayTemplate<E>) super.clone();
    clone._embeddedList = (ArrayList<Object>) _embeddedList.clone();
    return clone;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SpecificList copy() throws CloneNotSupportedException
  {
    SpecificElementArrayTemplate<E> copy = (SpecificElementArrayTemplate<E>) super.clone();
    copy._embeddedList = copyList(_embeddedList);
    return copy;
  }

  @Override
  public int size()
  {
    return _embeddedList.size();
  }

  @Override
  public boolean isEmpty()
  {
    return _embeddedList.isEmpty();
  }

  @Override
  public boolean contains(Object value)
  {
    return _embeddedList.contains(value);
  }

  @Override
  public Iterator<Object> iterator()
  {
    return _embeddedList.iterator();
  }

  @Override
  public ListIterator<Object> listIterator()
  {
    return _embeddedList.listIterator();
  }

  @Override
  public ListIterator<Object> listIterator(int index)
  {
    return _embeddedList.listIterator(index);
  }

  @Override
  public void forEach(Consumer<? super Object> action)
  {
    _embeddedList.forEach(action);
  }
}
