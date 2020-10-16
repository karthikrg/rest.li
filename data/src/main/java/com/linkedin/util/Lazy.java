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

package com.linkedin.util;

import java.util.function.Supplier;


/**
 * A thread safe lazy initialization wrapper.
 */
public class Lazy<T>
{
  private volatile T _value;
  private final Supplier<T> _supplier;

  public Lazy(Supplier<T> supplier)
  {
    _supplier = supplier;
  }

  public Lazy(T value)
  {
    _supplier = () -> value;
  }

  public T get()
  {
    if (_value == null)
    {
      synchronized (this)
      {
        if (_value == null)
        {
          _value = _supplier.get();
        }
      }
    }

    return _value;
  }
}
