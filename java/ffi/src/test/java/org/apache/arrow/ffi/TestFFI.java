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

import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;

public abstract class TestFFI {
  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    try {
      allocator.close();
    } catch (IllegalStateException e) {
      e.printStackTrace();
    }
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  protected StructVector createDummyStructVector() {
    FieldType type = new FieldType(true, Struct.INSTANCE, null, null);
    StructVector vector = new StructVector("struct", allocator, type, null);
    IntVector a = vector.addOrGet("a", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
    IntVector b = vector.addOrGet("b", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
    IntVector c = vector.addOrGet("c", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
    IntVector d = vector.addOrGet("d", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
    for (int j = 0; j < 5; j++) {
      a.setSafe(j, j);
      b.setSafe(j, j);
      c.setSafe(j, j);
      d.setSafe(j, j);
      vector.setIndexDefined(j);
    }
    a.setValueCount(5);
    b.setValueCount(5);
    c.setValueCount(5);
    d.setValueCount(5);
    vector.setValueCount(5);
    return vector;
  }

  protected VectorSchemaRoot createDummyVSR() {

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");
    IntVector a = new IntVector("a", new FieldType(true, new ArrowType.Int(32, true), null, metadata), allocator);
    IntVector b = new IntVector("b", allocator);
    IntVector c = new IntVector("c", allocator);
    IntVector d = new IntVector("d", allocator);

    for (int j = 0; j < 5; j++) {
      a.setSafe(j, j);
      b.setSafe(j, j);
      c.setSafe(j, j);
      d.setSafe(j, j);
    }
    a.setValueCount(5);
    b.setValueCount(5);
    c.setValueCount(5);
    d.setValueCount(5);

    List<Field> fields = Arrays.asList(a.getField(), b.getField(), c.getField(), d.getField());
    List<FieldVector> vectors = Arrays.asList(a, b, c, d);
    VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors);

    return vsr;
  }

  private static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
    return (VarCharVector)
            FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
  }

  protected VectorSchemaRoot createDummyVSRWithDictionary() {
    final byte[] zero = "zero".getBytes(StandardCharsets.UTF_8);
    final byte[] one = "one".getBytes(StandardCharsets.UTF_8);
    final byte[] two = "two".getBytes(StandardCharsets.UTF_8);
    try (final VarCharVector dictionaryVector = newVarCharVector("dictionary", allocator)) {
      final DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

      dictionaryVector.allocateNew(512, 3);
      dictionaryVector.setSafe(0, zero, 0, zero.length);
      dictionaryVector.setSafe(1, one, 0, one.length);
      dictionaryVector.setSafe(2, two, 0, two.length);
      dictionaryVector.setValueCount(3);

      final Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
      provider.put(dictionary);

      final FieldVector encodedVector;
      try (final VarCharVector unencoded = newVarCharVector("encoded", allocator)) {
        unencoded.allocateNewSafe();
        unencoded.set(1, one);
        unencoded.set(2, two);
        unencoded.set(3, zero);
        unencoded.set(4, two);
        unencoded.setValueCount(6);
        encodedVector = (FieldVector) DictionaryEncoder.encode(unencoded, dictionary);
      }

      final List<Field> fields = Collections.singletonList(encodedVector.getField());
      final List<FieldVector> vectors = Collections.singletonList(encodedVector);

      try (final VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVector.getValueCount())) {
        return root;
      }
    }
  }
}
