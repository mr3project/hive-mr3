/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

public class StringUtils {
  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  /**
   * Return an internalized string, or null if the given string is null.
   *
   * <p>This intentionally uses a Hive-local weak interner instead of
   * {@link String#intern()}. Metastore thrift objects call this method for many
   * fields while deserializing and copying objects (partition parameters, field
   * schemas, storage descriptor locations, etc). Routing every value through
   * the JVM string pool can make {@code String.intern()} a profiling hotspot. A
   * weak interner keeps the memory-deduplication benefit for equal live strings
   * without paying the cost of the JVM-wide string table.</p>
   *
   * @param str The string to intern
   * @return An equal canonical string cached by the metastore interner.
   */
  public static String intern(String str) {
    if (str == null) {
      return null;
    }
    return STRING_INTERNER.intern(str);
  }

  /**
   * Return an interned list with identical contents as the given list.
   * @param list The list whose strings will be interned
   * @return An identical list with its strings interned.
   */
  public static List<String> intern(List<String> list) {
    if(list == null) {
      return null;
    }
    List<String> newList = new ArrayList<>(list.size());
    for(String str : list) {
      newList.add(intern(str));
    }
    return newList;
  }

  /**
   * Return a map with interned keys and identical values.
   *
   * <p>Metastore parameter maps have a small set of repeated keys, but their
   * values are often table/partition-specific. Interning only the keys keeps the
   * high-value deduplication while avoiding many low-value interner lookups for
   * values.</p>
   *
   * @param map The map whose keys will be interned
   * @return An identical map with its keys interned.
   */
  public static Map<String, String> intern(Map<String, String> map) {
    if (map == null) {
      return null;
    }

    if (map.isEmpty()) {
      // nothing to intern
      return map;
    }
    Map<String, String> newMap = new HashMap<>(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      newMap.put(intern(entry.getKey()), entry.getValue());
    }
    return newMap;
  }

  public static Set<String> asSet(String... elements) {
    if (elements == null) return new HashSet<>();
    Set<String> set = new HashSet<>(elements.length);
    Collections.addAll(set, elements);
    return set;
  }

  /**
   * Normalize all identifiers to make equality comparisons easier.
   * @param identifier identifier
   * @return normalized version, with white space removed and all lower case.
   */
  public static String normalizeIdentifier(String identifier) {
    return identifier.trim().toLowerCase();
  }

  /**
   * Make a string representation of the exception.
   * @param e The exception to stringify
   * @return A string with exception name and call stack.
   * @deprecated
   */
  @Deprecated
  public static String stringifyException(Throwable e) {
    StringWriter stm = new StringWriter();
    PrintWriter wrt = new PrintWriter(stm);
    e.printStackTrace(wrt);
    wrt.close();
    return stm.toString();
  }

  /**
   * Given an array of bytes it will convert the bytes to a hex string
   * representation of the bytes.
   * @param bytes Input bytes
   * @param start start index, inclusively
   * @param end end index, exclusively
   * @return hex string representation of the byte array
   */
  public static String byteToHexString(byte[] bytes, int start, int end) {
    return org.apache.hadoop.util.StringUtils.byteToHexString(bytes, start, end);
  }

  /**
   * Checks if the input string/char sequence is empty
   * @param cs Input char sequence
   * @return true if empty and false if not
   */
  public static boolean isEmpty(CharSequence cs) {
    return cs == null || cs.length() == 0;
  }
}
