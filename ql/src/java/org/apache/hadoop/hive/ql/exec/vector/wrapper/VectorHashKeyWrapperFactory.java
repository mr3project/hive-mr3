/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector.wrapper;

import org.apache.hadoop.hive.ql.exec.vector.wrapper.VectorHashKeyWrapperBase.HashContext;

public class VectorHashKeyWrapperFactory {

  /** Wrapper implementation selected from the compiled key layout. */
  public enum KeyWrapperType {
    EMPTY,
    SINGLE_LONG,
    TWO_LONG,
    THREE_LONG,
    SINGLE_STRING,
    TWO_STRING,
    THREE_STRING,
    SINGLE_LONG_SINGLE_STRING,
    SINGLE_LONG_TWO_STRING,
    TWO_LONG_SINGLE_STRING,
    GENERAL_LONG_STRING,
    GENERAL
  }

  public static KeyWrapperType classify(int longValuesCount,
      int doubleValuesCount, int byteValuesCount, int decimalValuesCount, int timestampValuesCount,
      int intervalDayTimeValuesCount) {

    final int nonLongBytesCount =
        doubleValuesCount + decimalValuesCount +
        timestampValuesCount + intervalDayTimeValuesCount;

    if (nonLongBytesCount == 0) {
      if (byteValuesCount == 0) {
        if (longValuesCount == 1) {
          return KeyWrapperType.SINGLE_LONG;
        } else if (longValuesCount == 2) {
          return KeyWrapperType.TWO_LONG;
        } else if (longValuesCount == 3) {
          return KeyWrapperType.THREE_LONG;
        } else if (longValuesCount == 0) {
          return KeyWrapperType.EMPTY;
        }
      } else if (byteValuesCount == 1) {
        if (longValuesCount == 0) {
          return KeyWrapperType.SINGLE_STRING;
        } else if (longValuesCount == 1) {
          return KeyWrapperType.SINGLE_LONG_SINGLE_STRING;
        } else if (longValuesCount == 2) {
          return KeyWrapperType.TWO_LONG_SINGLE_STRING;
        }
      } else if (byteValuesCount == 2) {
        if (longValuesCount == 0) {
          return KeyWrapperType.TWO_STRING;
        } else if (longValuesCount == 1) {
          return KeyWrapperType.SINGLE_LONG_TWO_STRING;
        }
      } else if (byteValuesCount == 3) {
        if (longValuesCount == 0) {
          return KeyWrapperType.THREE_STRING;
        }
      }
      return KeyWrapperType.GENERAL_LONG_STRING;
    }

    return KeyWrapperType.GENERAL;
  }

  public static VectorHashKeyWrapperBase allocate(HashContext ctx, int longValuesCount,
      int doubleValuesCount, int byteValuesCount, int decimalValuesCount, int timestampValuesCount,
      int intervalDayTimeValuesCount, int keyCount) {
    return allocate(classify(longValuesCount, doubleValuesCount, byteValuesCount, decimalValuesCount,
        timestampValuesCount, intervalDayTimeValuesCount), ctx, longValuesCount, doubleValuesCount,
        byteValuesCount, decimalValuesCount, timestampValuesCount, intervalDayTimeValuesCount, keyCount);
  }

  static VectorHashKeyWrapperBase allocate(KeyWrapperType keyWrapperType, HashContext ctx,
      int longValuesCount, int doubleValuesCount, int byteValuesCount, int decimalValuesCount,
      int timestampValuesCount, int intervalDayTimeValuesCount, int keyCount) {
    switch (keyWrapperType) {
    case EMPTY:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return VectorHashKeyWrapperEmpty.EMPTY_KEY_WRAPPER;
    case SINGLE_LONG:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return new VectorHashKeyWrapperSingleLong();
    case TWO_LONG:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return new VectorHashKeyWrapperTwoLong();
    case THREE_LONG:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return new VectorHashKeyWrapperThreeLong();
    case SINGLE_STRING:
      return new VectorHashKeyWrapperSingleString(ctx);
    case TWO_STRING:
      return new VectorHashKeyWrapperTwoString(ctx);
    case THREE_STRING:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return new VectorHashKeyWrapperThreeString(ctx);
    case SINGLE_LONG_SINGLE_STRING:
      return new VectorHashKeyWrapperSingleLongSingleString(ctx);
    case SINGLE_LONG_TWO_STRING:
      return new VectorHashKeyWrapperSingleLongTwoString(ctx);
    case TWO_LONG_SINGLE_STRING:
      return new VectorHashKeyWrapperTwoLongSingleString(ctx);
    case GENERAL_LONG_STRING:
      assert false : "Allocated directly by VectorHashKeyWrapperBatch.allocateKeyWrappers()";
      return new VectorHashKeyWrapperGeneralLongString(ctx, longValuesCount, byteValuesCount, keyCount);
    case GENERAL:
    default:
      return new VectorHashKeyWrapperGeneral(ctx, longValuesCount, doubleValuesCount,
          byteValuesCount, decimalValuesCount, timestampValuesCount, intervalDayTimeValuesCount, keyCount);
    }
  }
}
