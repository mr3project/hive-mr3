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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/** Loops keep the concrete wrapper type at each per-row call site. */
final class VectorHashKeyWrapperBatchSpecializations {
  private VectorHashKeyWrapperBatchSpecializations() {
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (!columnVector.isNull[row]) {
        wrappers[i].assignLong(index, columnVector.vector[row]);
      } else {
        wrappers[i].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsRepeating(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignNullLong(keyIndex, index);
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int keyIndex, int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      if (!columnVector.isNull[r]) {
        wrappers[r].assignLong(index, columnVector.vector[r]);
      } else {
        wrappers[r].assignNullLong(keyIndex, index);
      }
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsRepeating(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[0]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector, int[] selected) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[selected[r]]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperSingleLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperTwoLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignLongNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeLong[] wrappers,
      int index, int size, LongColumnVector columnVector) {
    for (int r = 0; r < size; ++r) {
      wrappers[r].assignLong(index, columnVector.vector[r]);
    }
  }

  static void assignStringNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (columnVector.isNull[row]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[row],
            columnVector.start[row],
            columnVector.length[row]);
      }
    }
  }

  static void assignStringNullsNoRepeatingSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      if (columnVector.isNull[row]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[row],
            columnVector.start[row],
            columnVector.length[row]);
      }
    }
  }

  static void assignStringNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      if (columnVector.isNull[i]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[i],
            columnVector.start[i],
            columnVector.length[i]);
      }
    }
  }

  static void assignStringNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      if (columnVector.isNull[i]) {
        wrappers[i].assignNullString(keyIndex, index);
      } else {
        wrappers[i].assignString(
            index,
            columnVector.vector[i],
            columnVector.start[i],
            columnVector.length[i]);
      }
    }
  }

  static void assignStringNoNullsRepeating(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[0],
          columnVector.start[0],
          columnVector.length[0]);
    }
  }

  static void assignStringNoNullsRepeating(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[0],
          columnVector.start[0],
          columnVector.length[0]);
    }
  }

  static void assignStringNoNullsNoRepeatingSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      wrappers[i].assignString(
          index,
          columnVector.vector[row],
          columnVector.start[row],
          columnVector.length[row]);
    }
  }

  static void assignStringNoNullsNoRepeatingSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector, int[] selected) {
    for (int i = 0; i < size; ++i) {
      int row = selected[i];
      wrappers[i].assignString(
          index,
          columnVector.vector[row],
          columnVector.start[row],
          columnVector.length[row]);
    }
  }

  static void assignStringNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[i],
          columnVector.start[i],
          columnVector.length[i]);
    }
  }

  static void assignStringNoNullsNoRepeatingNoSelection(VectorHashKeyWrapperThreeString[] wrappers,
      int index, int size, BytesColumnVector columnVector) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignString(
          index,
          columnVector.vector[i],
          columnVector.start[i],
          columnVector.length[i]);
    }
  }

  static void clearIsNull(VectorHashKeyWrapperGeneralLongString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperSingleLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperTwoLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperThreeLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void clearIsNull(VectorHashKeyWrapperThreeString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].clearIsNull();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperGeneralLongString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperSingleLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperTwoLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperThreeLong[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void setHashKeys(VectorHashKeyWrapperThreeString[] wrappers, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].setHashKey();
    }
  }

  static void assignNullStrings(VectorHashKeyWrapperGeneralLongString[] wrappers,
      int keyIndex, int index, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignNullString(keyIndex, index);
    }
  }

  static void assignNullStrings(VectorHashKeyWrapperThreeString[] wrappers,
      int keyIndex, int index, int size) {
    for (int i = 0; i < size; ++i) {
      wrappers[i].assignNullString(keyIndex, index);
    }
  }
}
