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
package org.apache.hadoop.hive.ql.exec.vector;

import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchEncoding.HAS_NULLS;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchEncoding.IS_DECIMAL_64;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchEncoding.IS_REPEATING;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchEncoding.putInt;
import static org.apache.hadoop.hive.ql.exec.vector.VectorShuffleBatchEncoding.putLong;

import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;

/**
 * Producer-side, per-partition compact shuffle accumulation. All pending values are packed owned
 * bytes, never source vectors or copied execution batches. Schema plans and representation metadata
 * are shared; callers flush every partition before installing a different representation.
 */
public final class VectorShuffleBatchBuilder {

  /** Maximum buffered rows per partition; larger slices use the direct serializer. */
  public static final int ACCUMULATION_ROW_LIMIT = 8;

  private final int[] sourceColumnMap;
  private final NodePlan[] plans;
  private final PartitionBuffer[] partitions;
  private final VectorShuffleBatchMetadata metadata;
  private final int bitmapLength;
  // Column count, zeroed repeating bitmap, and owned date/timestamp metadata snapshot.
  private final byte[] headerTemplate;
  private final byte[] candidateMetadata;
  private byte[] fallback = new byte[0];
  private final BytesWritable fallbackValue = new BytesWritable();
  // One emission plan is enough: the writer consumes each frame synchronously.
  private final boolean[] repeatingColumns;
  private PartitionBuffer preparedPartition;
  private int frameSize;
  private int repeatingLength;

  public VectorShuffleBatchBuilder(ColumnVector[] columns, int[] sourceColumnMap,
      int partitionCount) {
    assert ACCUMULATION_ROW_LIMIT > 0 && ACCUMULATION_ROW_LIMIT % Byte.SIZE == 0;
    assert partitionCount > 1;
    this.sourceColumnMap = sourceColumnMap.clone();
    plans = new NodePlan[sourceColumnMap.length];
    for (int i = 0; i < plans.length; i++) {
      plans[i] = new NodePlan(columns[sourceColumnMap[i]]);
    }
    metadata = new VectorShuffleBatchMetadata(columns, this.sourceColumnMap);
    bitmapLength = (plans.length + 7) / 8;
    headerTemplate = new byte[Integer.BYTES + bitmapLength + metadata.getByteCount()];
    putInt(headerTemplate, 0, plans.length);
    candidateMetadata = new byte[metadata.getByteCount()];
    metadata.write(columns, headerTemplate, Integer.BYTES + bitmapLength);
    partitions = new PartitionBuffer[partitionCount];
    repeatingColumns = new boolean[plans.length];
  }

  /** Checks even unused nested nodes: their flags and metadata are part of the schema contract. */
  public boolean isCompatible(ColumnVector[] columns) {
    metadata.write(columns, candidateMetadata, 0);
    if (!Arrays.equals(candidateMetadata, 0, candidateMetadata.length,
        headerTemplate, Integer.BYTES + bitmapLength, headerTemplate.length)) {
      return false;
    }
    for (int i = 0; i < plans.length; i++) {
      if (!plans[i].matches(columns[sourceColumnMap[i]])) {
        return false;
      }
    }
    return true;
  }

  public void updateRepresentation(ColumnVector[] columns) {
    for (PartitionBuffer partition : partitions) {
      assert partition == null || partition.rows == 0;
    }
    for (int i = 0; i < plans.length; i++) {
      plans[i].update(columns[sourceColumnMap[i]]);
    }
    for (PartitionBuffer partition : partitions) {
      if (partition != null) {
        for (NodeBuffer column : partition.columns) {
          column.prepareRepresentation();
        }
      }
    }
    preparedPartition = null;
    metadata.write(columns, headerTemplate, Integer.BYTES + bitmapLength);
  }

  public int getRowCount(int partition) {
    PartitionBuffer pending = partitions[partition];
    return pending == null ? 0 : pending.rows;
  }

  /**
   * Copies a small slice into owned storage. The caller emits a full buffer before appending
   * another slice, and sends large remaining slices through the direct serializer.
   */
  public void append(int partition, VectorizedRowBatch source, int[] rowIndices,
      int rowOffset, int rowCount) {
    PartitionBuffer pending = partitions[partition];
    if (pending == null) {
      pending = new PartitionBuffer(plans);
      partitions[partition] = pending;
    }
    preparedPartition = null;
    assert rowCount > 0 && rowOffset >= 0;
    assert rowIndices != null && rowOffset <= rowIndices.length - rowCount;
    assert pending.rows < ACCUMULATION_ROW_LIMIT;
    assert pending.rows <= ACCUMULATION_ROW_LIMIT - rowCount;
    for (int column = 0; column < plans.length; column++) {
      pending.columns[column].append(source.cols[sourceColumnMap[column]],
          rowIndices, rowOffset, rowCount);
    }
    pending.rows += rowCount;
  }

  public int getFrameSize(int partition) {
    PartitionBuffer pending = partitions[partition];
    assert pending != null;
    assert pending.rows > 0 && pending.rows <= ACCUMULATION_ROW_LIMIT;
    if (preparedPartition == pending) {
      return frameSize;
    }
    long size = (long) headerTemplate.length + Integer.BYTES + Byte.BYTES;
    long savings = 0;
    long repeatedBytes = 0;
    for (int i = 0; i < pending.columns.length; i++) {
      NodeBuffer column = pending.columns[i];
      assert column.rows == pending.rows;
      long ordinarySize = column.ordinarySize();
      long repeatingSize = Byte.BYTES +
          (column.representativeNull ? 1 : column.representativeLength);
      repeatingColumns[i] = column.plan.scalar() && column.repeatingEligible &&
          pending.rows > 1 && ordinarySize > repeatingSize;
      size += ordinarySize;
      if (repeatingColumns[i]) {
        savings += ordinarySize - repeatingSize;
        repeatedBytes += repeatingSize;
      }
    }
    // All beneficial scalar columns share one optional repeating-section length.
    if (savings > Integer.BYTES) {
      size -= savings - Integer.BYTES;
      repeatingLength = Math.toIntExact(repeatedBytes);
    } else {
      Arrays.fill(repeatingColumns, false);
      repeatingLength = 0;
    }
    frameSize = Math.toIntExact(size);
    preparedPartition = pending;
    return frameSize;
  }

  /** Assembles an independently decodable frame without changing the pending partition. */
  public int writeFrame(int partition, byte[] destination, int offset) {
    PartitionBuffer pending = partitions[partition];
    int size = getFrameSize(partition);
    if (offset < 0 || size > destination.length - (long) offset) {
      throw new ArrayIndexOutOfBoundsException("Compact shuffle frame does not fit: " + size);
    }
    System.arraycopy(headerTemplate, 0, destination, offset, headerTemplate.length);
    int position = offset + headerTemplate.length;
    if (repeatingLength > 0) {
      putInt(destination, position, repeatingLength);
      position += Integer.BYTES;
      for (int i = 0; i < pending.columns.length; i++) {
        if (repeatingColumns[i]) {
          destination[offset + Integer.BYTES + (i >>> 3)] |= 1 << (i & 7);
          position = pending.columns[i].writeRepeating(destination, position);
        }
      }
    }
    putInt(destination, position, pending.rows);
    position += Integer.BYTES;
    destination[position++] = 0; // accumulated complex columns never contain nested repetition
    for (int i = 0; i < pending.columns.length; i++) {
      if (!repeatingColumns[i]) {
        position = pending.columns[i].writeOrdinary(destination, position);
      }
    }
    assert position - offset == size;
    return size;
  }

  /** The collector must consume this borrowed reusable slice synchronously. */
  public BytesWritable getFallbackFrame(int partition) {
    int size = getFrameSize(partition);
    fallback = grow(fallback, size);
    writeFrame(partition, fallback, 0);
    fallbackValue.setDirect(fallback, 0, size);
    return fallbackValue;
  }

  /** Called only after the corresponding writer operation succeeds. */
  public void reset(int partition) {
    preparedPartition = null;
    partitions[partition].reset();
  }

  public void discard() {
    preparedPartition = null;
    for (PartitionBuffer partition : partitions) {
      if (partition != null) {
        partition.reset();
      }
    }
  }

  private static byte[] grow(byte[] bytes, int required) {
    if (required <= bytes.length) {
      return bytes;
    }
    int capacity = (int) Math.min(Integer.MAX_VALUE,
        Math.max((long) required, Math.max(16L, (long) bytes.length * 2)));
    return Arrays.copyOf(bytes, capacity);
  }

  /** One shared descriptor per logical schema node, including duplicate mapped columns. */
  private static final class NodePlan {
    private final NodePlan[] children;
    private Class<?> representation;
    private int fixedWidth;
    private int flags;
    private boolean scalar;

    private NodePlan(ColumnVector column) {
      children = new NodePlan[childCount(column)];
      for (int i = 0; i < children.length; i++) {
        children[i] = new NodePlan(child(column, i));
      }
      update(column);
    }

    private boolean scalar() {
      return scalar;
    }

    private boolean matches(ColumnVector column) {
      if (representation != column.getClass()) {
        return false;
      }
      assert children.length == childCount(column);
      for (int i = 0; i < children.length; i++) {
        if (!children[i].matches(child(column, i))) {
          return false;
        }
      }
      return true;
    }

    private void update(ColumnVector column) {
      assert children.length == childCount(column);
      representation = column.getClass();
      fixedWidth = VectorShuffleBatchEncoding.fixedWidth(column);
      flags = column instanceof Decimal64ColumnVector ? IS_DECIMAL_64 : 0;
      // Empty structs/unions are complex too; scalar subclasses use the same encoding helpers.
      scalar = !(column instanceof StructColumnVector || column instanceof UnionColumnVector
          || column instanceof MultiValuedColumnVector);
      for (int i = 0; i < children.length; i++) {
        children[i].update(child(column, i));
      }
    }
  }

  private static int childCount(ColumnVector column) {
    if (column instanceof ListColumnVector) {
      return 1;
    }
    if (column instanceof MapColumnVector) {
      return 2;
    }
    if (column instanceof StructColumnVector) {
      return ((StructColumnVector) column).fields.length;
    }
    if (column instanceof UnionColumnVector) {
      return ((UnionColumnVector) column).fields.length;
    }
    return 0;
  }

  private static ColumnVector child(ColumnVector column, int index) {
    if (column instanceof ListColumnVector) {
      return ((ListColumnVector) column).child;
    }
    if (column instanceof MapColumnVector) {
      MapColumnVector map = (MapColumnVector) column;
      return index == 0 ? map.keys : map.values;
    }
    return column instanceof StructColumnVector ? ((StructColumnVector) column).fields[index]
        : ((UnionColumnVector) column).fields[index];
  }

  private static final class PartitionBuffer {
    private int rows;
    private final NodeBuffer[] columns;

    private PartitionBuffer(NodePlan[] plans) {
      columns = new NodeBuffer[plans.length];
      for (int i = 0; i < plans.length; i++) {
        columns[i] = new NodeBuffer(plans[i], true);
      }
    }

    private void reset() {
      rows = 0;
      for (NodeBuffer column : columns) {
        column.reset();
      }
    }
  }

  /** Packed ordinary payload and independent null mask for a schema node's active values. */
  private static final class NodeBuffer {
    private static final NodeBuffer[] NO_CHILDREN = new NodeBuffer[0];
    private static final byte[] NO_PAYLOAD = new byte[0];
    private final NodePlan plan;
    private final NodeBuffer[] children;
    private final boolean topLevel;
    private byte[] nulls = new byte[(ACCUMULATION_ROW_LIMIT + 7) / 8];
    private byte[] payload;
    private int rows;
    private int nullCount;
    private int length;
    private boolean repeatingEligible = true;
    private boolean representativeNull;
    // The first value's bytes at payload[0..representativeLength) are already owned and remain
    // stable while later ordinary values are appended, even if payload storage grows.
    private int representativeLength;

    private NodeBuffer(NodePlan plan, boolean topLevel) {
      this.plan = plan;
      this.topLevel = topLevel;
      children = plan.children.length == 0 ? NO_CHILDREN : new NodeBuffer[plan.children.length];
      for (int i = 0; i < children.length; i++) {
        children[i] = new NodeBuffer(plan.children[i], false);
      }
      int width = plan.scalar() ? Math.max(0, plan.fixedWidth) : Integer.BYTES;
      payload = width == 0 ? NO_PAYLOAD : new byte[Math.multiplyExact(ACCUMULATION_ROW_LIMIT, width)];
    }

    private void prepareRepresentation() {
      assert rows == 0;
      int width = plan.scalar() ? Math.max(0, plan.fixedWidth) : Integer.BYTES;
      payload = grow(payload, Math.multiplyExact(ACCUMULATION_ROW_LIMIT, width));
      for (NodeBuffer nested : children) {
        nested.prepareRepresentation();
      }
    }

    private void append(ColumnVector source, int[] indices, int offset, int count) {
      if (count == 0) {
        return;
      }
      int nextRows = Math.addExact(rows, count);
      // A bitmap is emitted only if the accumulated column contains a null. Reserve once
      // for the slice, including trailing non-null rows after an earlier null.
      if (nullCount > 0 || !source.noNulls) {
        nulls = grow(nulls, (int) ((nextRows + 7L) / 8));
      }
      if (!plan.scalar()) {
        for (int i = 0; i < count; i++) {
          appendComplex(source, indices == null ? offset + i : indices[offset + i]);
        }
        assert rows == nextRows;
        return;
      }
      if (source.isRepeating) {
        appendRepeating(source, count);
        assert rows == nextRows;
        return;
      }
      if (plan.fixedWidth >= 0) {
        payload = grow(payload, Math.toIntExact(length + (long) count * plan.fixedWidth));
      }
      final boolean hasNulls = !source.noNulls;
      // Dispatch once per column slice. Long/date/decimal64 and double loops reserve their
      // complete capacity above, so individual values need no encoding or growth checks.
      if (source instanceof LongColumnVector) {
        long[] values = ((LongColumnVector) source).vector;
        for (int i = 0; i < count; i++) {
          int index = indices == null ? offset + i : indices[offset + i];
          int start = length;
          boolean isNull = hasNulls && source.isNull[index];
          if (isNull) {
            markNull();
          } else {
            putLong(payload, length, values[index]);
            length += Long.BYTES;
          }
          finishScalar(isNull, start);
        }
      } else if (source instanceof DoubleColumnVector) {
        double[] values = ((DoubleColumnVector) source).vector;
        for (int i = 0; i < count; i++) {
          int index = indices == null ? offset + i : indices[offset + i];
          int start = length;
          boolean isNull = hasNulls && source.isNull[index];
          if (isNull) {
            markNull();
          } else {
            putLong(payload, length, Double.doubleToLongBits(values[index]));
            length += Double.BYTES;
          }
          finishScalar(isNull, start);
        }
      } else if (source instanceof BytesColumnVector) {
        BytesColumnVector values = (BytesColumnVector) source;
        for (int i = 0; i < count; i++) {
          int index = indices == null ? offset + i : indices[offset + i];
          int start = length;
          boolean isNull = hasNulls && source.isNull[index];
          if (isNull) {
            markNull();
          } else {
            int size = values.length[index];
            assert size >= 0;
            int end = Math.toIntExact(length + (long) Integer.BYTES + size);
            payload = grow(payload, end);
            System.arraycopy(values.vector[index], values.start[index],
                payload, length + Integer.BYTES, size);
            putInt(payload, length, size);
            length = end;
          }
          finishScalar(isNull, start);
        }
      } else {
        for (int i = 0; i < count; i++) {
          int index = indices == null ? offset + i : indices[offset + i];
          int start = length;
          boolean isNull = hasNulls && source.isNull[index];
          if (isNull) {
            markNull();
          } else {
            appendScalar(source, index);
          }
          finishScalar(isNull, start);
        }
      }
      assert rows == nextRows;
    }

    private void appendRepeating(ColumnVector source, int count) {
      boolean isNull = !source.noNulls && source.isNull[0];
      int start = length;
      if (isNull) {
        for (int i = rows; i < rows + count; i++) {
          nulls[i >>> 3] |= 1 << (i & 7);
        }
        nullCount += count;
      } else {
        appendScalar(source, 0);
        int width = length - start;
        int end = Math.toIntExact(start + (long) count * width);
        payload = grow(payload, end);
        // Keep the required ordinary payload, but encode and compare the source value once.
        if (width > 0) {
          for (int copied = 1; copied < count;) {
            int copies = Math.min(copied, count - copied);
            System.arraycopy(payload, start, payload, start + copied * width, copies * width);
            copied += copies;
          }
        }
        // Compare just the first encoded value; every value in this slice is identical.
        finishScalar(false, start);
        length = end;
        rows += count - 1;
        return;
      }
      finishScalar(true, start);
      rows += count - 1;
    }

    private void markNull() {
      nulls[rows >>> 3] |= 1 << (rows & 7);
      nullCount++;
    }

    private void finishScalar(boolean isNull, int start) {
      if (topLevel && repeatingEligible) {
        if (rows == 0) {
          representativeNull = isNull;
          representativeLength = length - start;
        } else if (representativeNull != isNull || (!isNull &&
            (representativeLength != length - start ||
            !Arrays.equals(payload, 0, representativeLength, payload, start, length)))) {
          repeatingEligible = false;
        }
      }
      rows++;
    }

    private void appendComplex(ColumnVector source, int index) {
      assert !plan.scalar();
      index = source.isRepeating ? 0 : index;
      final boolean isNull = !source.noNulls && source.isNull[index];
      if (isNull) {
        markNull();
      } else if (source instanceof StructColumnVector) {
        for (int i = 0; i < children.length; i++) {
          children[i].append(((StructColumnVector) source).fields[i], null, index, 1);
        }
      } else if (source instanceof UnionColumnVector) {
        UnionColumnVector union = (UnionColumnVector) source;
        int tag = union.tags[index];
        assert tag >= 0 && tag < children.length;
        appendInt(tag);
        children[tag].append(union.fields[tag], null, index, 1);
      } else if (source instanceof MultiValuedColumnVector) {
        MultiValuedColumnVector collection = (MultiValuedColumnVector) source;
        int count = Math.toIntExact(collection.lengths[index]);
        int offset = Math.toIntExact(collection.offsets[index]);
        assert count >= 0 && offset >= 0;
        appendInt(count);
        for (int i = 0; i < children.length; i++) {
          children[i].append(child(source, i), null, offset, count);
        }
      } else {
        throw new IllegalArgumentException("Unsupported vector shuffle type " + source.getClass());
      }
      rows++;
    }

    private void appendInt(int value) {
      payload = grow(payload, Math.addExact(length, Integer.BYTES));
      putInt(payload, length, value);
      length += Integer.BYTES;
    }

    private void appendScalar(ColumnVector source, int index) {
      int width = plan.fixedWidth;
      if (source instanceof BytesColumnVector) {
        width = Math.addExact(Integer.BYTES, ((BytesColumnVector) source).length[index]);
      }
      payload = grow(payload, Math.addExact(length, Math.max(0, width)));
      while (true) {
        try {
          length += VectorShuffleBatchEncoding.writeScalar(source, index, payload, length);
          return;
        } catch (ArrayIndexOutOfBoundsException ex) {
          // Decimal.writeDirect has variable size and uses checked array accesses. All other
          // scalar payloads have already reserved their exact size above.
          if (!(source instanceof DecimalColumnVector)) {
            throw ex;
          }
          payload = grow(payload, Math.addExact(payload.length, 1));
        }
      }
    }

    private long ordinarySize() {
      long size = Byte.BYTES + (nullCount == 0 ? 0 : (rows + 7L) / 8) + length;
      for (NodeBuffer nested : children) {
        size += nested.ordinarySize();
      }
      return size;
    }

    private int writeRepeating(byte[] destination, int position) {
      assert topLevel && plan.scalar() && repeatingEligible && rows > 1;
      destination[position++] = (byte) (plan.flags | IS_REPEATING |
          (representativeNull ? HAS_NULLS : 0));
      if (representativeNull) {
        destination[position++] = 1;
      } else {
        System.arraycopy(payload, 0, destination, position, representativeLength);
        position += representativeLength;
      }
      return position;
    }

    private int writeOrdinary(byte[] destination, int position) {
      destination[position++] = (byte) (plan.flags | (nullCount > 0 ? HAS_NULLS : 0));
      if (nullCount > 0) {
        int bitmapBytes = (int) ((rows + 7L) / 8);
        System.arraycopy(nulls, 0, destination, position, bitmapBytes);
        position += bitmapBytes;
      }
      System.arraycopy(payload, 0, destination, position, length);
      position += length;
      // TODO: recursively compress repetition in accumulated complex columns.
      // Source repetition was honored during append; every accumulated child is ordinary.
      for (NodeBuffer nested : children) {
        position = nested.writeOrdinary(destination, position);
      }
      return position;
    }

    private void reset() {
      if (nullCount > 0) {
        Arrays.fill(nulls, 0, (int) ((rows + 7L) / 8), (byte) 0);
      }
      rows = 0;
      nullCount = 0;
      length = 0;
      repeatingEligible = true;
      representativeNull = false;
      representativeLength = 0;
      for (NodeBuffer nested : children) {
        nested.reset();
      }
    }
  }
}
