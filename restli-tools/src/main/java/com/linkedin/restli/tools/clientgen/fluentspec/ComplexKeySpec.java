/*
   Copyright (c) 2021 LinkedIn Corp.

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

package com.linkedin.restli.tools.clientgen.fluentspec;

import com.linkedin.restli.common.ComplexResourceKey;
import java.util.Map;
import org.apache.commons.lang.ClassUtils;


/**
 * Spec for complex key, used by {@link CollectionResourceSpec}
 */
@SuppressWarnings({"rawtypes"})
public class ComplexKeySpec
{
  private final String _keyKeyClassName;
  private Boolean _useShortKeyKeyClassName;
  private final String _paramKeyClassName;
  private Boolean _useShortParamKeyClassName;
  private final BaseResourceSpec _baseResourceSpec;
  private String _parameterizedSignature = null;

  public final Class keyClass = ComplexResourceKey.class;

  public ComplexKeySpec(String keyKeyType, String paramKeyType, BaseResourceSpec baseResourceSpec)
  {
    this._baseResourceSpec = baseResourceSpec;
    this._keyKeyClassName = _baseResourceSpec.getJavaBindTypeName(keyKeyType);
    this._paramKeyClassName = _baseResourceSpec.getJavaBindTypeName(paramKeyType);
  }

  public String getParameterizedSignature()
  {
    if (_parameterizedSignature == null)
    {
      _parameterizedSignature = String.format("ComplexResourceKey<%s, %s>",
          getKeyKeyClassDisplayName(),
          getParamKeyClassDisplayName()
      );
    }
    return _parameterizedSignature;
  }

  public String getKeyKeyClassName()
  {
    return _keyKeyClassName;
  }

  public String getKeyKeyClassDisplayName()
  {
    if (_useShortKeyKeyClassName == null)
    {
      _useShortKeyKeyClassName =
          !SpecUtils.checkIfShortNameConflictAndUpdateMapping(_baseResourceSpec.getImportCheckConflict(),
              ClassUtils.getShortClassName(_keyKeyClassName), _keyKeyClassName);
    }
    return _useShortKeyKeyClassName ? ClassUtils.getShortClassName(_keyKeyClassName): _keyKeyClassName;
  }

  public String getParamKeyClassName()
  {
    return _paramKeyClassName;
  }

  public String getParamKeyClassDisplayName()
  {
    if (_useShortParamKeyClassName == null)
    {
      _useShortParamKeyClassName =
          !SpecUtils.checkIfShortNameConflictAndUpdateMapping(_baseResourceSpec.getImportCheckConflict(),
              ClassUtils.getShortClassName(_paramKeyClassName), _paramKeyClassName);
    }
    return _useShortParamKeyClassName? ClassUtils.getShortClassName(_paramKeyClassName): _paramKeyClassName;
  }

  public void setUseShortKeyKeyClassName(boolean useShortKeyKeyClassName)
  {
    this._useShortKeyKeyClassName = useShortKeyKeyClassName;
  }

  public void setUseShortParamKeyClassName(boolean useShortParamKeyClassName)
  {
    this._useShortParamKeyClassName = useShortParamKeyClassName;
  }
}