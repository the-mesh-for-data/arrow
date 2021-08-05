/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.ffi;

import static org.apache.arrow.ffi.NativeUtil.NULL;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Importer for {@link ArrowArray}.
 */
final class ArrayImporter {
  private static final int MAX_IMPORT_RECURSION_LEVEL = 64;

  final FieldVector vector;
  ReferenceManager referenceManager;
  private int recursionLevel;

  ArrayImporter(FieldVector vector) {
    this.vector = vector;
  }

  void importArray(BufferAllocator allocator, ArrowArray src, Field field) {
    ArrowArray.Snapshot snapshot = src.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");

    // Move imported array
    ArrowArray ownedArray = ArrowArray.allocateNew(allocator);
    ownedArray.save(snapshot);
    src.markReleased();
    src.close();

    recursionLevel = 0;
    // This keeps the array alive as long as there are any buffers that need it
    referenceManager = new FFIReferenceManager(ownedArray);
    doImport(snapshot, field);
  }

  private void importChild(ArrayImporter parent, ArrowArray src) {
    ArrowArray.Snapshot snapshot = src.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");
    recursionLevel = parent.recursionLevel + 1;
    checkState(recursionLevel < MAX_IMPORT_RECURSION_LEVEL, "Recursion level in ArrowArray struct exceeded");
    // Child buffers will keep the entire parent import alive.
    // Perhaps we can move the child structs on import,
    // but that is another level of complication.
    referenceManager = parent.referenceManager;
    doImport(snapshot, null); // assumes no dictionary
  }

  private void doImport(ArrowArray.Snapshot snapshot, Field field) {
    // First import children (required for reconstituting parent array data)
    long[] children = NativeUtil.toJavaArray(snapshot.children, checkedCastToInt(snapshot.n_children));
    if (children != null) {
      List<FieldVector> childVectors = vector.getChildrenFromFields();
      checkState(children.length == childVectors.size(), "ArrowArray struct has %s children (expected %s)",
          children.length, childVectors.size());
      for (int i = 0; i < children.length; i++) {
        checkState(children[i] != NULL, "ArrowArray struct has NULL child at position %s", i);
        ArrayImporter childImporter = new ArrayImporter(childVectors.get(i));
        childImporter.importChild(this, ArrowArray.wrap(children[i]));
      }
    }

    if (snapshot.dictionary != NULL) {
      ArrowArray dictionary = ArrowArray.wrap(snapshot.dictionary);

      // now I need to create an ArrayImporter for the dictionary.
      // I need to initialize it with a newly allocated FieldVector ??
      // then doImport to populate this new FieldVector
      // ArrayImporter dictionaryImporter = new ArrayImporter(XXX);

      // Also I need to allocate an array (FieldVector ?) into which I will place
      // the results of the decode() operation.
      // Then, I need to copy the contents of that array in vector

      Dictionary d = new Dictionary(null, field.getDictionary());
      DictionaryEncoder.decode(null, d);
    }

    // TODO: support dictionary import

    // Import main data
    ArrowFieldNode fieldNode = new ArrowFieldNode(snapshot.length, snapshot.null_count);
    List<ArrowBuf> buffers = importBuffers(snapshot);
    try {
      vector.loadFieldBuffers(fieldNode, buffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Could not load buffers for field " + vector.getField() + ". error message: " + e.getMessage(), e);
    }
  }

  private List<ArrowBuf> importBuffers(ArrowArray.Snapshot snapshot) {
    long[] buffers = NativeUtil.toJavaArray(snapshot.buffers, checkedCastToInt(snapshot.n_buffers));
    if (buffers == null) {
      return new ArrayList<>();
    }

    int buffersCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
    checkState(buffers.length == buffersCount, "Expected %d buffers for imported type %s, ArrowArray struct has %d",
        buffersCount, vector.getField().getType().getTypeID(), buffers.length);

    List<ArrowBuf> result = new ArrayList<>(buffersCount);
    for (long bufferPtr : buffers) {
      ArrowBuf buffer = null;
      if (bufferPtr != NULL) {
        int bufferSize = vector.getBufferSizeFor(checkedCastToInt(snapshot.length));
        buffer = new ArrowBuf(referenceManager, null, bufferSize, bufferPtr);
      }
      result.add(buffer);
    }
    return result;
  }
}
