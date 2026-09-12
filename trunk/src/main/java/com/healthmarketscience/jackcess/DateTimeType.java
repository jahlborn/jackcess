/*
Copyright (c) 2018 James Ahlborn

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

package com.healthmarketscience.jackcess;

/**
 * Enum for selecting how a Database returns date/time types.  Prefer using
 * {@link DateTimeType#LOCAL_DATE_TIME} as using Date is being phased out and
 * will eventually be removed.
 *
 * @author James Ahlborn
 */
public enum DateTimeType
{
  /** Use legacy {@link java.util.Date} objects.  This was the default for
      Jackcess before version 3.5. */
  DATE,
  /** Use jdk8+ {@link java.time.LocalDateTime} objects.  This is the default
      for Jackcess from version 3.5 onwards. */
  LOCAL_DATE_TIME;
}
