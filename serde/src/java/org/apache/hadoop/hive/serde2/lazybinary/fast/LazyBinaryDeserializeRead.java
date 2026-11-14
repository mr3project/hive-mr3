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

package org.apache.hadoop.hive.serde2.lazybinary.fast;

import static org.apache.tez.util.FastByteComparisons.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.tez.util.FastByteComparisons.theUnsafe;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.fast.DeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Directly deserialize the fast LazyBinary format with fixed-width integers and lengths.
 * This format differs from the variable-length format used by LazyBinarySerDe.
 *
 * The caller is responsible for calling the read method for the right type of each field
 * (after calling readNextField).
 *
 * Reading some fields require a results object to receive value information.  A separate
 * results object is created by the caller at initialization per different field even for the same
 * type.
 *
 * Some type values are by reference to either bytes in the deserialization buffer or to
 * other type specific buffers.  So, those references are only valid until the next time set is
 * called.
 */
public final class LazyBinaryDeserializeRead extends DeserializeRead {
  public static final Logger LOG = LoggerFactory.getLogger(LazyBinaryDeserializeRead.class.getName());

  private byte[] bytes;
  private int start;
  private int offset;
  private int end;

  private boolean skipLengthPrefix = false;

  private final Timestamp timestamp = new Timestamp();

  private static final int INITIAL_STACK_SIZE = 16;

  private Field[] stack;
  private int stackDepth;
  private int currentGeneration;
  private Field root;
  private final boolean allTopLevelPrimitive;

  private class Field {
    Field[] children;

    Category category;
    PrimitiveCategory primitiveCategory;
    TypeInfo typeInfo;
    DataTypePhysicalVariation dataTypePhysicalVariation;

    int index;
    int count;
    int start;
    int end;
    int nullByteStart;
    byte nullByte;
    byte tag;
    int generation;
  }

  public LazyBinaryDeserializeRead(TypeInfo[] typeInfos, boolean useExternalBuffer) {
    this(typeInfos, null, useExternalBuffer);
  }

  public LazyBinaryDeserializeRead(TypeInfo[] typeInfos, DataTypePhysicalVariation[] dataTypePhysicalVariations,
      boolean useExternalBuffer) {
    super(typeInfos, dataTypePhysicalVariations, useExternalBuffer);
    currentExternalBufferNeeded = false;

    stack = new Field[INITIAL_STACK_SIZE];

    root = new Field();
    root.category = Category.STRUCT;
    root.children = createFields(typeInfos);
    root.count = typeInfos.length;

    boolean allPrimitive = true;
    for (Field field : root.children) {
      if (field.category != Category.PRIMITIVE) {
        allPrimitive = false;
        break;
      }
    }
    allTopLevelPrimitive = allPrimitive;
  }

  private Field[] createFields(TypeInfo[] typeInfos) {
    final Field[] children = new Field[typeInfos.length];
    for (int i = 0; i < typeInfos.length; i++) {
      children[i] = createField(typeInfos[i]);
    }
    return children;
  }

  private Field createField(TypeInfo typeInfo) {
    final Field field = new Field();
    final Category category = typeInfo.getCategory();
    field.category = category;
    field.typeInfo = typeInfo;
    switch (category) {
    case PRIMITIVE:
      field.primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      break;
    case LIST:
      field.children = new Field[1];
      field.children[0] = createField(((ListTypeInfo) typeInfo).getListElementTypeInfo());
      break;
    case MAP:
      field.children = new Field[2];
      field.children[0] = createField(((MapTypeInfo) typeInfo).getMapKeyTypeInfo());
      field.children[1] = createField(((MapTypeInfo) typeInfo).getMapValueTypeInfo());
      break;
    case STRUCT:
      final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
      final List<TypeInfo> fieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
      field.children = createFields(fieldTypeInfos.toArray(new TypeInfo[fieldTypeInfos.size()]));
      break;
    case UNION:
      final UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
      final List<TypeInfo> objectTypeInfos = unionTypeInfo.getAllUnionObjectTypeInfos();
      field.children = createFields(objectTypeInfos.toArray(new TypeInfo[objectTypeInfos.size()]));
      break;
    default:
      throw new RuntimeException();
    }
    return field;
  }

  /*
   * Set the range of bytes to be deserialized.
   */
  @Override
  public void set(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    start = offset;
    end = offset + length;

    nextGeneration();
    prepareField(root);
    resetStack();
  }

  private void nextGeneration() {
    currentGeneration++;
    if (currentGeneration == 0) {
      clearGeneration(root);
      currentGeneration = 1;
    }
  }

  private void clearGeneration(Field field) {
    field.generation = 0;
    if (field.children == null) {
      return;
    }
    for (Field child : field.children) {
      clearGeneration(child);
    }
  }

  private void prepareField(Field field) {
    if (field.generation != currentGeneration) {
      field.generation = currentGeneration;
      field.index = 0;
      field.tag = 0;
      field.nullByte = 0;
    }
  }

  private void resetStack() {
    stackDepth = 1;
    stack[0] = root;
  }

  private Field peekStack() {
    return stack[stackDepth - 1];
  }

  private void pushStack(Field field) {
    if (stackDepth == stack.length) {
      stack = Arrays.copyOf(stack, stack.length * 2);
    }
    stack[stackDepth++] = field;
  }

  private Field popStack() {
    stackDepth--;
    if (stackDepth == 0) {
      throw new RuntimeException();
    }
    return stack[stackDepth - 1];
  }

  /*
   * Get detailed read position information to help diagnose exceptions.
   */
  public String getDetailedReadPositionString() {
    StringBuilder sb = new StringBuilder(64);

    sb.append("Reading byte[] of length ");
    sb.append(bytes.length);
    sb.append(" at start offset ");
    sb.append(start);
    sb.append(" for length ");
    sb.append(end - start);
    sb.append(" to read ");
    sb.append(root.children.length);
    sb.append(" fields with types ");
    sb.append(Arrays.toString(typeInfos));
    sb.append(".  Read field #");
    sb.append(root.index);
    sb.append(" at field start position ");
    sb.append(root.start);
    sb.append(" current read offset ");
    sb.append(offset);

    return sb.toString();
  }

  /*
   * Reads the the next field.
   *
   * Afterwards, reading is positioned to the next field.
   *
   * @return  Return true when the field was not null and data is put in the appropriate
   *          current* member.
   *          Otherwise, false when the field is null.
   *
   */
  @Override
  public boolean readNextField() throws IOException {
    if (!allTopLevelPrimitive) {
      return readComplexField();
    }

    // A flat schema never pushes a nested field onto the stack.
    assert stackDepth == 1 && stack[0] == root;
    final int fieldIndex = root.index;
    if ((fieldIndex & 7) == 0) {
      root.nullByte = bytes[offset++];
    }
    if ((root.nullByte & (1 << (fieldIndex & 7))) == 0) {
      root.index++;
      return false;
    }

    final boolean hasValue = readPrimitive(root.children[fieldIndex]);
    root.index++;
    if (offset > end) {
      throw new EOFException();
    }
    return hasValue;
  }

  private boolean readPrimitive(Field field) throws IOException {
    final PrimitiveCategory primitiveCategory = field.primitiveCategory;
    final TypeInfo typeInfo = field.typeInfo;
    switch (primitiveCategory) {
    case BOOLEAN:
      // No check needed for single byte read.
      currentBoolean = (bytes[offset++] != 0);
      break;
    case BYTE:
      // No check needed for single byte read.
      currentByte = bytes[offset++];
      break;
    case SHORT:
      // Last item -- ok to be at end.
      if (offset + 2 > end) {
        throw new EOFException();
      }
      currentShort = LazyBinaryUtils.byteArrayToShort(bytes, offset);
      offset += 2;
      break;
    case INT:
      currentInt = readInt();
      break;
    case LONG:
      currentLong = readLong();
      break;
    case FLOAT:
      currentFloat = Float.intBitsToFloat(readInt());
      break;
    case DOUBLE:
      currentDouble = Double.longBitsToDouble(readLong());
      break;

    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        int length = readInt();
        int saveStart = offset;
        offset += length;
        // Last item -- ok to be at end.
        if (offset > end) {
          throw new EOFException();
        }

        currentBytes = bytes;
        currentBytesStart = saveStart;
        currentBytesLength = length;
      }
      break;
    case DATE:
      currentDateWritable.set(readInt());
      break;
    case TIMESTAMP:
      {
        long seconds = readLong();
        int nanos = readInt();
        timestamp.setTimeInSeconds(seconds, nanos);
        currentTimestampWritable.set(timestamp);
      }
      break;
    case INTERVAL_YEAR_MONTH:
      currentHiveIntervalYearMonthWritable.set(readInt());
      break;
    case INTERVAL_DAY_TIME:
      {
        long seconds = readLong();
        int nanos = readInt();
        currentHiveIntervalDayTimeWritable.set(seconds, nanos);
      }
      break;
    case DECIMAL:
      {
        // Since enforcing precision and scale can cause a HiveDecimal to become NULL,
        // we must read it, enforce it here, and either return NULL or buffer the result.

        int readScale = readInt();
        int length = readInt();
        int saveStart = offset;
        offset += length;
        // Last item -- ok to be at end.
        if (offset > end) {
          throw new EOFException();
        }
        currentHiveDecimalWritable.setFromBigIntegerBytesAndScale(
            bytes, saveStart, length, readScale);
        boolean decimalIsNull = !currentHiveDecimalWritable.isSet();
        if (!decimalIsNull) {

          final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;

          final int precision = decimalTypeInfo.getPrecision();
          final int scale = decimalTypeInfo.getScale();

          decimalIsNull = !currentHiveDecimalWritable.mutateEnforcePrecisionScale(precision, scale);
          if (!decimalIsNull) {
            return true;
          }
        }
        return false;
      }
    default:
      throw new Error("Unexpected primitive category " + primitiveCategory.name());
    }
    return true;
  }

  private int readInt() throws EOFException {
    if (offset + Integer.BYTES > end) {
      throw new EOFException();
    }
    int value = Integer.reverseBytes(theUnsafe.getInt(bytes, BYTE_ARRAY_BASE_OFFSET + offset));
    offset += Integer.BYTES;
    return value;
  }

  private long readLong() throws EOFException {
    if (offset + Long.BYTES > end) {
      throw new EOFException();
    }
    long value = Long.reverseBytes(theUnsafe.getLong(bytes, BYTE_ARRAY_BASE_OFFSET + offset));
    offset += Long.BYTES;
    return value;
  }

  private void skipPrimitive(Field field) throws EOFException {
    switch (field.primitiveCategory) {
    case BOOLEAN:
    case BYTE:
      offset++;
      break;
    case SHORT:
      offset += Short.BYTES;
      break;
    case INT:
    case FLOAT:
    case DATE:
    case INTERVAL_YEAR_MONTH:
      offset += Integer.BYTES;
      break;
    case LONG:
    case DOUBLE:
      offset += Long.BYTES;
      break;
    case TIMESTAMP:
    case INTERVAL_DAY_TIME:
      offset += Long.BYTES + Integer.BYTES;
      break;
    case BINARY:
    case STRING:
    case CHAR:
    case VARCHAR:
      {
        final int length = readInt();
        offset += length;
      }
      break;
    case DECIMAL:
      {
        offset += Integer.BYTES;
        final int length = readInt();
        offset += length;
      }
      break;
    default:
      throw new Error("Unexpected primitive category " + field.primitiveCategory.name());
    }
  }

  /*
   * Reads through an undesired field.
   *
   * No data values are valid after this call.
   * Designed for skipping columns that are not included.
   */
  public void skipNextField() throws IOException {
    final Field current = peekStack();
    prepareField(current);
    final boolean isNull = isNull(current);

    if (isNull) {
      current.index++;
      return;
    }

    if (readUnionTag(current)) {
      current.index++;
      return;
    }

    final Field child = getChild(current);

    if (child.category == Category.PRIMITIVE) {
      skipPrimitive(child);
    } else {
      assert !skipLengthPrefix;
      // The prefix covers the complete payload, including nested fields.
      final int length = LazyBinaryUtils.byteArrayToInt(bytes, offset);
      offset += Integer.BYTES + length;
    }
    current.index++;

    if (offset > end) {
      throw new EOFException();
    }
  }

  /*
   * Call this method may be called after all the all fields have been read to check
   * for unread fields.
   *
   * Note that when optimizing reading to stop reading unneeded include columns, worrying
   * about whether all data is consumed is not appropriate (often we aren't reading it all by
   * design).
   *
   * Since LazySimpleDeserializeRead parses the line through the last desired column it does
   * support this function.
   */
  public boolean isEndOfInputReached() {
    return (offset == end);
  }

  private boolean isNull(Field field) {
    final int bit = 1 << (field.index & 7);
    switch (field.category) {
    case PRIMITIVE:
    case UNION:
      return false;
    case LIST:
    case MAP:
      return (bytes[field.nullByteStart + (field.index >>> 3)] & bit) == 0;
    case STRUCT:
      if ((field.index & 7) == 0) {
        field.nullByte = bytes[offset++];
      }
      return (field.nullByte & bit) == 0;
    default:
      throw new RuntimeException();
    }
  }

  private void parseHeader(Field field) {
    // Init
    field.index = 0;
    field.start = offset;

    // Read length
    if (!skipLengthPrefix) {
      final int length = LazyBinaryUtils.byteArrayToInt(bytes, offset);
      offset += Integer.BYTES;
      field.end = offset + length;
    }

    switch (field.category) {
    case LIST:
    case MAP:
      // Read count
      final int count = LazyBinaryUtils.byteArrayToInt(bytes, offset);
      offset += Integer.BYTES;
      if (field.category == Category.LIST) {
        field.count = count;
      } else {
        field.count = count * 2;
      }

      // Null byte start
      field.nullByteStart = offset;
      offset += ((field.count) + 7) / 8;
      break;
    case STRUCT:
      field.count = ((StructTypeInfo) field.typeInfo).getAllStructFieldTypeInfos().size();
      break;
    case UNION:
      field.count = 2;
      break;
    }
  }

  private Field getChild(Field field) {
    switch (field.category) {
    case LIST:
      return field.children[0];
    case MAP:
      return field.children[field.index % 2];
    case STRUCT:
      return field.children[field.index];
    case UNION:
      return field.children[field.tag];
    default:
      throw new RuntimeException();
    }
  }

  private boolean readUnionTag(Field field) {
    if (field.category == Category.UNION && field.index == 0) {
      field.tag = bytes[offset++];
      currentInt = field.tag;
      return true;
    } else {
      return false;
    }
  }

  // Push or next
  @Override
  public boolean readComplexField() throws IOException {
    final Field current = peekStack();
    prepareField(current);
    boolean isNull = isNull(current);

    if (isNull) {
      current.index++;
      return false;
    }

    if (readUnionTag(current)) {
      current.index++;
      return true;
    }

    final Field child = getChild(current);

    if (child.category == Category.PRIMITIVE) {
      isNull = !readPrimitive(child);
      current.index++;
    } else {
      parseHeader(child);
      prepareField(child);
      pushStack(child);
    }

    if (offset > end) {
      throw new EOFException();
    }
    return !isNull;
  }

  // Pop (list, map)
  @Override
  public boolean isNextComplexMultiValue() {
    Field current = peekStack();
    prepareField(current);
    final boolean isNext = current.index < current.count;
    if (!isNext) {
      popStack().index++;
    }
    return isNext;
  }

  // Pop (struct, union)
  @Override
  public void finishComplexVariableFieldsType() {
    popStack().index++;
  }
}
