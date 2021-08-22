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

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.NullableLargeVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRoundtrip {
  private static final String EMPTY_SCHEMA_PATH = "";
  private RootAllocator allocator = null;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  FieldVector vectorRoundtrip(FieldVector vector) {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

      // Producer creates structures from exisitng memory pointers
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
          ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
        // Producer exports vector into the FFI structures
        FFI.exportVector(allocator, vector, arrowArray, arrowSchema);
      }

      // Consumer imports vector
      return FFI.importVector(allocator, consumerArrowArray, consumerArrowSchema);
    }
  }

  boolean roundtrip(FieldVector vector, Class<?> clazz) {
    try (ValueVector imported = vectorRoundtrip(vector)) {
      assertTrue(clazz.isInstance(imported), String.format("expected %s but was %s", clazz, imported.getClass()));
      return VectorEqualsVisitor.vectorEquals(vector, imported);
    }
  }

  @Test
  public void testBitVector() {
    BitVector imported;

    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024);
      vector.setValueCount(1024);

      // Put and set a few values
      vector.set(0, 1);
      vector.set(1, 0);
      vector.set(100, 0);
      vector.set(1022, 1);

      vector.setValueCount(1024);

      imported = (BitVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }

    assertEquals(1, imported.get(0));
    assertEquals(0, imported.get(1));
    assertEquals(0, imported.get(100));
    assertEquals(1, imported.get(1022));
    assertEquals(1020, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testIntVector() {
    IntVector imported;
    try (final IntVector vector = new IntVector("v", allocator)) {
      setVector(vector, 1, 2, 3, null);
      imported = (IntVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testBigIntVector() {
    BigIntVector imported;
    try (final BigIntVector vector = new BigIntVector("v", allocator)) {
      setVector(vector, 1L, 2L, 3L, null);
      imported = (BigIntVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDateDayVector() {
    DateDayVector imported;
    try (final DateDayVector vector = new DateDayVector("v", allocator)) {
      setVector(vector, 1, 2, 3, null);
      imported = (DateDayVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDateMilliVector() {
    DateMilliVector imported;
    try (final DateMilliVector vector = new DateMilliVector("v", allocator)) {
      setVector(vector, 1L, 2L, 3L, null);
      imported = (DateMilliVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDecimalVector() {
    try (final DecimalVector vector = new DecimalVector("v", allocator, 1, 1)) {
      setVector(vector, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, DecimalVector.class));
    }
  }

  @Test
  public void testDurationVector() {
    for (TimeUnit unit : TimeUnit.values()) {
      final FieldType fieldType = FieldType.nullable(new ArrowType.Duration(unit));
      try (final DurationVector vector = new DurationVector("v", fieldType, allocator)) {
        setVector(vector, 1L, 2L, 3L, null);
        assertTrue(roundtrip(vector, DurationVector.class));
      }
    }
  }

  @Test
  public void testZeroVectorEquals() {
    try (final ZeroVector vector = new ZeroVector()) {
      // A ZeroVector is imported as a NullVector
      assertTrue(roundtrip(vector, NullVector.class));
    }
  }

  @Test
  public void testFixedSizeBinaryVector() {
    try (final FixedSizeBinaryVector vector = new FixedSizeBinaryVector("v", allocator, 2)) {
      setVector(vector, new byte[] { 0b0000, 0b0001 }, new byte[] { 0b0010, 0b0011 });
      assertTrue(roundtrip(vector, FixedSizeBinaryVector.class));
    }
  }

  @Test
  public void testFloat4Vector() {
    try (final Float4Vector vector = new Float4Vector("v", allocator)) {
      setVector(vector, 0.1f, 0.2f, 0.3f, null);
      assertTrue(roundtrip(vector, Float4Vector.class));
    }
  }

  @Test
  public void testFloat8Vector() {
    try (final Float8Vector vector = new Float8Vector("v", allocator)) {
      setVector(vector, 0.1d, 0.2d, 0.3d, null);
      assertTrue(roundtrip(vector, Float8Vector.class));
    }
  }

  @Test
  public void testIntervalDayVector() {
    try (final IntervalDayVector vector = new IntervalDayVector("v", allocator)) {
      IntervalDayHolder value = new IntervalDayHolder();
      value.days = 5;
      value.milliseconds = 100;
      setVector(vector, value, null);
      assertTrue(roundtrip(vector, IntervalDayVector.class));
    }
  }

  @Test
  public void testIntervalYearVector() {
    try (final IntervalYearVector vector = new IntervalYearVector("v", allocator)) {
      setVector(vector, 1990, 2000, 2010, 2020, null);
      assertTrue(roundtrip(vector, IntervalYearVector.class));
    }
  }

  @Test
  public void testSmallIntVector() {
    try (final SmallIntVector vector = new SmallIntVector("v", allocator)) {
      setVector(vector, (short) 0, (short) 256, null);
      assertTrue(roundtrip(vector, SmallIntVector.class));
    }
  }

  @Test
  public void testTimeMicroVector() {
    try (final TimeMicroVector vector = new TimeMicroVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeMicroVector.class));
    }
  }

  @Test
  public void testTimeMilliVector() {
    try (final TimeMilliVector vector = new TimeMilliVector("v", allocator)) {
      setVector(vector, 0, 1, 2, 3, null);
      assertTrue(roundtrip(vector, TimeMilliVector.class));
    }
  }

  @Test
  public void testTimeNanoVector() {
    try (final TimeNanoVector vector = new TimeNanoVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeNanoVector.class));
    }
  }

  @Test
  public void testTimeSecVector() {
    try (final TimeSecVector vector = new TimeSecVector("v", allocator)) {
      setVector(vector, 0, 1, 2, 3, null);
      assertTrue(roundtrip(vector, TimeSecVector.class));
    }
  }

  @Test
  public void testTimeStampMicroTZVector() {
    try (final TimeStampMicroTZVector vector = new TimeStampMicroTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMicroTZVector.class));
    }
  }

  @Test
  public void testTimeStampMicroVector() {
    try (final TimeStampMicroVector vector = new TimeStampMicroVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMicroVector.class));
    }
  }

  @Test
  public void testTimeStampMilliTZVector() {
    try (final TimeStampMilliTZVector vector = new TimeStampMilliTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMilliTZVector.class));
    }
  }

  @Test
  public void testTimeStampMilliVector() {
    try (final TimeStampMilliVector vector = new TimeStampMilliVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMilliVector.class));
    }
  }

  @Test
  public void testTimeTimeStampNanoTZVector() {
    try (final TimeStampNanoTZVector vector = new TimeStampNanoTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampNanoTZVector.class));
    }
  }

  @Test
  public void testTimeStampNanoVector() {
    try (final TimeStampNanoVector vector = new TimeStampNanoVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampNanoVector.class));
    }
  }

  @Test
  public void testTimeStampSecTZVector() {
    try (final TimeStampSecTZVector vector = new TimeStampSecTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampSecTZVector.class));
    }
  }

  @Test
  public void testTimeStampSecVector() {
    try (final TimeStampSecVector vector = new TimeStampSecVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampSecVector.class));
    }
  }

  @Test
  public void testTinyIntVector() {
    try (final TinyIntVector vector = new TinyIntVector("v", allocator)) {
      setVector(vector, (byte) 0, (byte) 1, null);
      assertTrue(roundtrip(vector, TinyIntVector.class));
    }
  }

  @Test
  public void testUInt1Vector() {
    try (final UInt1Vector vector = new UInt1Vector("v", allocator)) {
      setVector(vector, (byte) 0, (byte) 1, null);
      assertTrue(roundtrip(vector, UInt1Vector.class));
    }
  }

  @Test
  public void testUInt2Vector() {
    try (final UInt2Vector vector = new UInt2Vector("v", allocator)) {
      setVector(vector, '0', '1', null);
      assertTrue(roundtrip(vector, UInt2Vector.class));
    }
  }

  @Test
  public void testUInt4Vector() {
    try (final UInt4Vector vector = new UInt4Vector("v", allocator)) {
      setVector(vector, 0, 1, null);
      assertTrue(roundtrip(vector, UInt4Vector.class));
    }
  }

  @Test
  public void testUInt8Vector() {
    try (final UInt8Vector vector = new UInt8Vector("v", allocator)) {
      setVector(vector, 0L, 1L, null);
      assertTrue(roundtrip(vector, UInt8Vector.class));
    }
  }

  @Test
  public void testVarBinaryVector() {
    try (final VarBinaryVector vector = new VarBinaryVector("v", allocator)) {
      setVector(vector, "abc".getBytes(), "def".getBytes(), null);
      assertTrue(roundtrip(vector, VarBinaryVector.class));
    }
  }

  @Test
  public void testVarCharVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      setVector(vector, "abc", "def", null);
      assertTrue(roundtrip(vector, VarCharVector.class));
    }
  }

  @Test
  public void testLargeVarBinaryVector() {
    try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello world";
      try (ArrowBuf buf = allocator.buffer(16)) {
        buf.setBytes(0, str.getBytes());
        binHolder.start = 0;
        binHolder.end = str.length();
        binHolder.buffer = buf;
        vector.setSafe(0, binHolder);
        vector.setSafe(1, nullHolder);

        assertTrue(roundtrip(vector, LargeVarBinaryVector.class));
      }
    }
  }

  @Test
  public void testLargeVarCharVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      setVector(vector, "abc", "def", null);
      assertTrue(roundtrip(vector, LargeVarCharVector.class));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      setVector(vector, Arrays.stream(new int[] { 1, 2 }).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] { 3, 4 }).boxed().collect(Collectors.toList()), new ArrayList<Integer>());
      assertTrue(roundtrip(vector, ListVector.class));
    }
  }

  @Test
  public void testLargeListVector() {
    try (final LargeListVector vector = LargeListVector.empty("v", allocator)) {
      setVector(vector, Arrays.stream(new int[] { 1, 2 }).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] { 3, 4 }).boxed().collect(Collectors.toList()), new ArrayList<Integer>());
      assertTrue(roundtrip(vector, LargeListVector.class));
    }
  }

  @Test
  public void testFixedSizeListVector() {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("v", 2, allocator)) {
      setVector(vector, Arrays.stream(new int[] { 1, 2 }).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] { 3, 4 }).boxed().collect(Collectors.toList()));
      assertTrue(roundtrip(vector, FixedSizeListVector.class));
    }
  }

  @Test
  public void testMapVector() {
    int count = 5;
    try (final MapVector vector = MapVector.empty("v", allocator, false)) {
      vector.allocateNew();
      UnionMapWriter mapWriter = vector.getWriter();
      for (int i = 0; i < count; i++) {
        mapWriter.startMap();
        for (int j = 0; j < i + 1; j++) {
          mapWriter.startEntry();
          mapWriter.key().bigInt().writeBigInt(j);
          mapWriter.value().integer().writeInt(j);
          mapWriter.endEntry();
        }
        mapWriter.endMap();
      }
      mapWriter.setValueCount(count);

      assertTrue(roundtrip(vector, MapVector.class));
    }
  }

  @Test
  public void testUnionVector() {

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (UnionVector vector = UnionVector.empty("v", allocator)) {
      vector.allocateNew();

      // write some data
      vector.setType(0, MinorType.UINT4);
      vector.setSafe(0, uInt4Holder);
      vector.setType(2, MinorType.UINT4);
      vector.setSafe(2, uInt4Holder);
      vector.setValueCount(4);

      assertTrue(roundtrip(vector, UnionVector.class));
    }
  }

  @Test
  public void testStructVector() {
    try (final StructVector vector = StructVector.empty("v", allocator)) {
      Map<String, List<Integer>> data = new HashMap<>();
      data.put("col_1", Arrays.stream(new int[] { 1, 2 }).boxed().collect(Collectors.toList()));
      data.put("col_2", Arrays.stream(new int[] { 3, 4 }).boxed().collect(Collectors.toList()));
      setVector(vector, data);
      assertTrue(roundtrip(vector, StructVector.class));
    }
  }

  @Test
  public void testVectorSchemaRoot() {
    VectorSchemaRoot imported;

    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      try (VectorSchemaRoot vsr = createTestVSR()) {
        // Producer creates structures from exisitng memory pointers
        try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
            ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
          // Producer exports vector into the FFI structures
          FFI.exportVectorSchemaRoot(allocator, vsr, arrowArray, arrowSchema);
        }
      }
      // Consumer imports vector
      imported = FFI.importVectorSchemaRoot(allocator, consumerArrowSchema, consumerArrowArray);
    }

    // Ensure that imported VectorSchemaRoot is valid even after FFI structures
    // closed
    try (VectorSchemaRoot original = createTestVSR()) {
      assertTrue(imported.equals(original));
    }
    imported.close();
  }

  @Test
  public void testSchema() {
    Field decimalField = new Field("inner1", FieldType.nullable(new ArrowType.Decimal(19, 4, 128)), null);
    Field strField = new Field("inner2", FieldType.nullable(new ArrowType.Utf8()), null);
    Field itemField = new Field("col1", FieldType.nullable(new ArrowType.Struct()), List.of(decimalField, strField));
    Field intField = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(List.of(itemField, intField));
    // Consumer allocates empty ArrowSchema
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator)) {
      // Producer fills the schema with data
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress())) {
        FFI.exportSchema(allocator, schema, arrowSchema);
      }
      // Consumer imports schema
      Schema importedSchema = FFI.importSchema(consumerArrowSchema);
      assertEquals(schema.toJson(), importedSchema.toJson());
    }
  }

  @Test
  public void testImportReleasedArray() {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      // Producer creates structures from exisitng memory pointers
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
          ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
        // Producer exports vector into the FFI structures
        try (final NullVector vector = new NullVector()) {
          FFI.exportVector(allocator, vector, arrowArray, arrowSchema);
        }
      }

      // Release array structure
      consumerArrowArray.markReleased();

      // Consumer tried to imports vector but fails
      Exception e = assertThrows(IllegalStateException.class, () -> {
        FFI.importVector(allocator, consumerArrowArray, consumerArrowSchema);
      });

      assertEquals("Cannot import released ArrowArray", e.getMessage());
    }
  }

  private VectorSchemaRoot createTestVSR() {
    BitVector bitVector = new BitVector("boolean", allocator);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");
    FieldType fieldType = new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata);
    VarCharVector varCharVector = new VarCharVector("varchar", fieldType, allocator);

    bitVector.allocateNew();
    varCharVector.allocateNew();
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

    return new VectorSchemaRoot(fields, vectors);
  }

}
