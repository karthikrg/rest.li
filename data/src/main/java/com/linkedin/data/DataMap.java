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

package com.linkedin.data;

import com.linkedin.data.collections.CheckedMap;
import com.linkedin.data.collections.MapChecker;
import com.linkedin.data.collections.SpecificMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * An {@link DataMap} maps strings to Data objects.
 * <p>
 *
 * The key of this map is always a string. When a key is being added or replaced,
 * the map will verify that key is a string. If the key is not a string,
 * the map will throw an {@link IllegalArgumentException}.
 * <p>
 *
 * When an value is being added or replaced on the map, the map
 * will verify that the value is a Data object. If the value
 * is not a Data object, then the map will throw an {@link IllegalArgumentException}.
 * <p>
 *
 * Cloning via the {@link #clone()} method will shallow copy the {@link DataMap}
 * and will not deep copy contained complex objects. Copying via the {@link #copy()}
 * method will deep copy the {@link DataMap}, which includes deep copying the
 * contained complex objects. Keys are not deep copied because the keys are
 * immutable strings.
 * <p>
 *
 * Instrumentation if enabled is only enabled for the {@link DataMap} and not
 * for the underlying {@link CheckedMap}. Furthermore, if a {@link DataMap} is cloned,
 * the clone will not have instrumentation enabled and the clone's instrumented
 * data will be cleared.
 * <p>
 *
 * Since {@link DataMap} extends {@link CheckedMap}, copying of the {@link DataMap} is lazy and may be
 * delayed until the {@link DataMap} is about to be modified.
 *
 * @author slim
 */
public final class DataMap extends CheckedMap<String,Object> implements DataComplex
{
  public static final String RESERVED_CONSTANT_PREFIX = "**";
  public static final String RESERVED_CONSTANT_SUFFIX = "**";
  public static String reservedConstant(String name)
  {
    return RESERVED_CONSTANT_PREFIX + name + RESERVED_CONSTANT_SUFFIX;
  }

  public static final String ERROR_KEY = reservedConstant("ERROR");

  /**
   * Constructs an empty {@link DataMap}.
   */
  public DataMap()
  {
    super(_checker);
  }

  /**
   * Constructs a {@link DataMap} with the entries provided by the input map.
   *
   * @param map provides the entries of the new {@link DataMap}.
   */
  public DataMap(Map<? extends String, ? extends Object> map)
  {
    super(map, _checker);
  }

  /**
   * Constructs a {@link DataMap} with the specified initial capacity and
   * default load factor.
   *
   * @param initialCapacity provides the initial capacity of the {@link DataMap}.
   *
   * @see HashMap
   */
  public DataMap(int initialCapacity)
  {
    super(initialCapacity, _checker);
  }

  /**
   * Constructs a {@link DataMap} with the specified initial capacity and
   * load factor.
   *
   * @param initialCapacity provides the initial capacity of the {@link DataMap}.
   * @param loadFactor provides the load factor of the {@link DataMap}.
   *
   * @see HashMap
   */
  public DataMap(int initialCapacity, float loadFactor)
  {
    super(initialCapacity, loadFactor, _checker);
  }

  /**
   * Constructs a {@link DataMap} backed by the given specific map.
   *
   * <p>This constructor is meant to be invoked only from code-generated models.</p>
   *
   * @param specificMap provides the specific map.
   *
   * @see HashMap
   */
  public DataMap(SpecificMap specificMap)
  {
    super(specificMap, _checker, false);
    _encapsulatesSpecificMap = true;
  }

  /**
   * Constructs a {@link DataMap} backed by the given map without copying.
   *
   * <p>This constructor is meant to be invoked only from codecs.</p>
   *
   * @param map provides the map.
   */
  public DataMap(Map<String, Object> map, boolean noCopyPlaceholder)
  {
    super(map, _checker, noCopyPlaceholder);
    _encapsulatesSpecificMap = (map instanceof SpecificMap);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataMap clone() throws CloneNotSupportedException
  {
    DataMap o = (DataMap) super.clone();
    o._encapsulatesSpecificMap = _encapsulatesSpecificMap;
    o._madeReadOnly = false;
    o._instrumented = false;
    o._accessMap = null;
    o._dataComplexHashCode = 0;
    o._isTraversing = null;

    return o;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Map<String, Object> cloneMap(Map<String, Object> input) throws CloneNotSupportedException
  {
    if (input instanceof SpecificMap)
    {
      return (Map<String, Object>) ((SpecificMap) input).clone();
    }
    else
    {
      return super.cloneMap(input);
    }
  }

  @Override
  public Object get(Object key)
  {
    instrumentAccess(key);
    return super.get(key);
  }

  @Override
  public boolean containsKey(Object key)
  {
    instrumentAccess(key);
    return super.containsKey(key);
  }

  @Override
  public DataMap copy() throws CloneNotSupportedException
  {
    return Data.copy(this, new DataComplexTable());
  }

  /**
   * Deep copy this object and the complex Data objects referenced by this object.
   *
   * @param alreadyCopied provides the objects already copied, and their copies.
   * @throws CloneNotSupportedException if the referenced object cannot be copied.
   */
  public void copyReferencedObjects(DataComplexTable alreadyCopied) throws CloneNotSupportedException
  {
    for (Map.Entry<String,?> e : entrySet())
    {
      Object value = e.getValue();
      Object valueCopy = Data.copy(value, alreadyCopied);
      if (value != valueCopy)
      {
        putWithoutChecking(e.getKey(), valueCopy);
      }
    }
  }

  @Override
  public void makeReadOnly()
  {
    if (!_madeReadOnly)
    {
      for (Map.Entry<String,?> e : entrySet())
      {
        Data.makeReadOnly(e.getValue());
      }
      setReadOnly();
      _madeReadOnly = true;
    }
  }

  @Override
  public boolean isMadeReadOnly()
  {
    return _madeReadOnly;
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link Boolean},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link Boolean},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public Boolean getBoolean(String key)
  {
    return (Boolean) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link Integer},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link Integer},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public Integer getInteger(String key)
  {
    return (Integer) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link Long},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link Long},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public Long getLong(String key)
  {
    return (Long) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link Float},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link Float},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public Float getFloat(String key)
  {
    return (Float) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link Double},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link Double},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public Double getDouble(String key)
  {
    return (Double) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link String},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link String},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public String getString(String key)
  {
    return (String) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link ByteString},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link ByteString},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public ByteString getByteString(String key)
  {
    return (ByteString) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link DataMap},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link DataMap},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public DataMap getDataMap(String key)
  {
    return (DataMap) get(key);
  }

  /**
   * Returns the value to which the specified key is mapped and cast to {@link DataList},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @param key provides the key whose associated value is to be returned.
   * @return the value to which the specified key is mapped and cast to {@link DataList},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public DataList getDataList(String key)
  {
    return (DataList) get(key);
  }

  /**
   * Returns the value to which {@link #ERROR_KEY} is mapped and cast to {@link String},
   * or {@code null} if this map contains no mapping for the key.
   *
   * @return the value to which the specified key is mapped and cast to {@link String},
   *         or {@code null} if this map contains no mapping for the key.
   */
  public String getError()
  {
    return (String) get(ERROR_KEY);
  }

  /**
   * Adds an error message to the {@link DataMap}.
   *
   * If a value is not mapped to {@link #ERROR_KEY}, bind the specified error message
   * to {@link #ERROR_KEY}. Otherwise, replace the value of {@link #ERROR_KEY} with
   * a new {@link String} constructed by appending the specified error message to
   * the previous value of {@link #ERROR_KEY}.
   *
   * @param msg provides the error message to add.
   * @return the new value of {@link #ERROR_KEY}.
   */
  public String addError(String msg)
  {
    String error = getError();
    String res;
    if (error != null)
    {
      res = error + msg;
    }
    else
    {
      res = msg;
    }
    put(ERROR_KEY, res);
    return res;
  }

  @Override
  public void startInstrumentingAccess()
  {
    Data.startInstrumentingAccess(values());
    _instrumented = true;
    if (_accessMap == null)
    {
      _accessMap = new HashMap<String, Integer>();
    }
  }

  @Override
  public void stopInstrumentingAccess()
  {
    _instrumented = false;
    Data.stopInstrumentingAccess(values());
  }

  @Override
  public void clearInstrumentedData()
  {
    if (_accessMap != null)
    {
      _accessMap.clear();
    }
  }

  @Override
  public void collectInstrumentedData(StringBuilder keyPrefix, Map<String, Map<String, Object>> instrumentedData, boolean collectAllData)
  {
    for (Map.Entry<String, Object> entry : entrySet())
    {
      String key = entry.getKey();
      Integer timesAccessed = _accessMap == null ? null : _accessMap.get(key);
      if (timesAccessed == null)
      {
        timesAccessed = 0;
      }

      int preLength = keyPrefix.length();

      keyPrefix.append('.');
      keyPrefix.append(key);

      Data.collectInstrumentedData(keyPrefix, entry.getValue(), timesAccessed, instrumentedData, collectAllData);

      keyPrefix.setLength(preLength);
    }
  }

  @Override
  public int dataComplexHashCode()
  {
    if (_dataComplexHashCode != 0)
    {
      return _dataComplexHashCode;
    }

    synchronized (this)
    {
      if (_dataComplexHashCode == 0)
      {
        _dataComplexHashCode = DataComplexHashCode.nextHashCode();
      }
    }

    return _dataComplexHashCode;
  }

  public void traverse(Data.TraverseCallback callback, Data.CycleChecker cycleChecker) throws IOException
  {
    if (isEmpty())
    {
      callback.emptyMap();
      return;
    }

    try
    {
      cycleChecker.startMap(this);
      callback.startMap(this);
      Iterable<Map.Entry<String, Object>> orderedEntrySet = callback.orderMap(this);

      //
      // If the ordered entry set is null, then it means that we don't care about traversal order.
      //
      if (orderedEntrySet == null)
      {
        //
        // If this is backed by a specific map, delegate traversal to the specific map. Else, use Java 8 forEach
        // to avoid intermediary object creation.
        //
        if (_encapsulatesSpecificMap)
        {
          ((SpecificMap) _map).traverse(callback, cycleChecker);
        }
        else
        {
          try
          {
            forEach((key, value) ->
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
      }
      else
      {
        for (Map.Entry<String, Object> entry : orderedEntrySet)
        {
          callback.key(entry.getKey());
          Data.traverse(entry.getValue(), callback, cycleChecker);
        }
      }

      callback.endMap();
    }
    finally
    {
      cycleChecker.endMap(this);
    }
  }

  // Unit test use only
  void disableChecker()
  {
    super._checker = null;
  }

  // Unit test use only
  Map<String, Object> getUnderlying()
  {
    return getObject();
  }

  private void instrumentAccess(Object key)
  {
    if (_instrumented)
    {
      Integer i = _accessMap.get(key);
      _accessMap.put(key.toString(), (i == null ? 1 : i + 1));
    }
  }

  private final static MapChecker<String, Object> _checker = (map, key, value) -> {
    if (key.getClass() != String.class)
    {
      throw new IllegalArgumentException("Key must be a string");
    }
    Data.checkAllowed((DataComplex) map, value);
  };

  Object isTraversing()
  {
    return getOrCreateIsTraversing().get();
  }

  void setTraversing(Object value)
  {
    getOrCreateIsTraversing().set(value);
  }

  private ThreadLocal<Object> getOrCreateIsTraversing()
  {
    if (_isTraversing == null)
    {
      synchronized (this)
      {
        if (_isTraversing == null)
        {
          _isTraversing = new ThreadLocal<>();
        }
      }
    }

    return _isTraversing;
  }

  public SpecificMap getSpecificMap()
  {
    return _encapsulatesSpecificMap ? (SpecificMap) _map : null;
  }

  /**
   * Indicates if this {@link DataMap} is currently being traversed by a {@link Data.TraverseCallback} if this value is
   * not null, or not if this value is null. This is internally marked package private, used for cycle detection and
   * not meant for use by external callers. This is maintained as a {@link ThreadLocal} to allow for concurrent
   * traversals of the same {@link DataMap} from multiple threads.
   */
  private ThreadLocal<Object> _isTraversing;
  private boolean _madeReadOnly = false;
  private boolean _instrumented = false;
  private Map<String, Integer> _accessMap;
  private boolean _encapsulatesSpecificMap = false;
  int _dataComplexHashCode = 0;
}
