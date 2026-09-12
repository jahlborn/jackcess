/*
Copyright (c) 2026 James Ahlborn

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

package com.healthmarketscience.jackcess.impl;

import java.util.Properties;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Utility for abstracting loading system configuration.
 *
 * @author James Ahlborn
 */
public class SystemConfig
{

  private static final UnaryOperator<String> PROP_GETTER;
  private static final Supplier<Properties> PROPS_GETTER;

  static {
    if(Boolean.getBoolean("com.healthmarketscience.jackcess.testConfig")) {
      PROPS_GETTER = TestConfig.CONFIG::get;
      PROP_GETTER = p -> TestConfig.CONFIG.get().getProperty(p);
    } else {
      PROPS_GETTER = null;
      PROP_GETTER = System::getProperty;
    }
  }

  private SystemConfig() {}

  public static String getProperty(String propName) {
    return PROP_GETTER.apply(propName);
  }

  public static String getProperty(String propName, String defaultValue) {
    String value = getProperty(propName);
    return ((value != null) ? value : defaultValue);
  }

  /** for unit tests to set scoped "system" config values */
  public static AutoCloseable withProperty(String propName, String propValue) {
    Properties props = PROPS_GETTER.get();
    props.setProperty(propName, propValue);
    return () -> props.remove(propName);
  }

  /** allows unit tests to set "system" properties just for a single test */
  private static final class TestConfig {
    private static final ThreadLocal<Properties> CONFIG =
      ThreadLocal.withInitial(() -> new Properties(System.getProperties()));
  }
}
