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

package com.healthmarketscience.jackcess.impl.expr;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Utility for mapping Windows Locale Identifier (LCID) values to locale info.
 * Contains only the LCIDs that are known to appear in MS Access databases.
 *
 * @author James Ahlborn
 */
public class LocaleUtil
{
  private static final Map<Integer,LcidInfo> LCID_TO_INFO;

  static {
    Map<Integer,LcidInfo> map = new HashMap<Integer,LcidInfo>();
    // General / English
    add(map, 1033, "General", Locale.US);
    // Western European
    add(map, 1031, "German",     Locale.GERMAN);
    add(map, 1036, "French",     Locale.FRENCH);
    add(map, 1034, "Spanish",    lc("es"));
    add(map, 1040, "Italian",    Locale.ITALIAN);
    add(map, 1043, "Dutch",      lc("nl"));
    add(map, 1046, "Portuguese", lc("pt"));
    add(map, 1053, "Swedish",    lc("sv"));
    add(map, 1030, "Danish",     lc("da"));
    add(map, 1044, "Norwegian",  lc("no"));
    add(map, 1035, "Finnish",    lc("fi"));
    // Central/Eastern European
    add(map, 1045, "Polish",     lc("pl"));
    add(map, 1029, "Czech",      lc("cs"));
    add(map, 1038, "Hungarian",  lc("hu"));
    add(map, 1050, "Croatian",   lc("hr"));
    add(map, 1051, "Slovak",     lc("sk"));
    add(map, 1060, "Slovenian",  lc("sl"));
    add(map, 1048, "Romanian",   lc("ro"));
    add(map, 1026, "Bulgarian",  lc("bg"));
    // Cyrillic
    add(map, 1049, "Russian",    lc("ru"));
    add(map, 1058, "Ukrainian",  lc("uk"));
    // Baltic
    add(map, 1061, "Estonian",   lc("et"));
    add(map, 1062, "Latvian",    lc("lv"));
    add(map, 1063, "Lithuanian", lc("lt"));
    // Turkish and related
    add(map, 1055, "Turkish",    lc("tr"));
    add(map, 1068, "Azerbaijani",lc("az"));
    // Greek
    add(map, 1032, "Greek",      lc("el"));
    // East Asian
    add(map, 1041, "Japanese",         Locale.JAPANESE);
    add(map, 1042, "Korean",           Locale.KOREAN);
    add(map, 2052, "Chinese Simplified",  Locale.SIMPLIFIED_CHINESE);
    add(map, 1028, "Chinese Traditional", Locale.TRADITIONAL_CHINESE);
    // Arabic / Hebrew
    add(map, 1025, "Arabic", lc("ar"));
    add(map, 1037, "Hebrew", lc("he"));
    // Nordic/Romance
    add(map, 1069, "Basque",   lc("eu"));
    add(map, 1027, "Catalan",  lc("ca"));
    LCID_TO_INFO = map;
  }

  private LocaleUtil() {}

  public static LcidInfo getInfo(int localeId) {
    return LCID_TO_INFO.get(localeId);
  }

  private static Locale lc(String tag) {
    return Locale.forLanguageTag(tag);
  }

  private static void add(Map<Integer,LcidInfo> map, int lcid,
                          String name, Locale locale) {
    map.put(lcid, new LcidInfo(name, locale));
  }

  /**
   * Holds the display name and Java {@link Locale} for a Windows LCID.
   */
  public static final class LcidInfo
  {
    private final String _name;
    private final Locale _locale;

    private LcidInfo(String name, Locale locale) {
      _name = name;
      _locale = locale;
    }

    public Locale getLocale() {
      return _locale;
    }

    @Override
    public String toString() {
      return _name;
    }
  }
}
