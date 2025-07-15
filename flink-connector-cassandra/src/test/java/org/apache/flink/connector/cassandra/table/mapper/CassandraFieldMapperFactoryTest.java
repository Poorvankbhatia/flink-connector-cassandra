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

package org.apache.flink.connector.cassandra.table.mapper;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CassandraFieldMapperFactory}. */
class CassandraFieldMapperFactoryTest {

    @Test
    void testPrimitiveTypeMappers() {
        // Test boolean mapper
        CassandraFieldMapper booleanMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BooleanType());
        assertThat(booleanMapper).isInstanceOf(PrimitiveFieldMappers.BooleanMapper.class);

        // Test byte mapper
        CassandraFieldMapper byteMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TinyIntType());
        assertThat(byteMapper).isInstanceOf(PrimitiveFieldMappers.ByteMapper.class);

        // Test short mapper
        CassandraFieldMapper shortMapper =
                CassandraFieldMapperFactory.createFieldMapper(new SmallIntType());
        assertThat(shortMapper).isInstanceOf(PrimitiveFieldMappers.ShortMapper.class);

        // Test integer mapper
        CassandraFieldMapper intMapper =
                CassandraFieldMapperFactory.createFieldMapper(new IntType());
        assertThat(intMapper).isInstanceOf(PrimitiveFieldMappers.IntegerMapper.class);

        // Test long mapper
        CassandraFieldMapper longMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BigIntType());
        assertThat(longMapper).isInstanceOf(PrimitiveFieldMappers.LongMapper.class);

        // Test float mapper
        CassandraFieldMapper floatMapper =
                CassandraFieldMapperFactory.createFieldMapper(new FloatType());
        assertThat(floatMapper).isInstanceOf(PrimitiveFieldMappers.FloatMapper.class);

        // Test double mapper
        CassandraFieldMapper doubleMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DoubleType());
        assertThat(doubleMapper).isInstanceOf(PrimitiveFieldMappers.DoubleMapper.class);

        // Test string mapper
        CassandraFieldMapper stringMapper =
                CassandraFieldMapperFactory.createFieldMapper(VarCharType.STRING_TYPE);
        assertThat(stringMapper).isInstanceOf(PrimitiveFieldMappers.StringMapper.class);

        // Test date mapper
        CassandraFieldMapper dateMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DateType());
        assertThat(dateMapper).isInstanceOf(PrimitiveFieldMappers.DateMapper.class);

        // Test timestamp mapper
        CassandraFieldMapper timestampMapper =
                CassandraFieldMapperFactory.createFieldMapper(new TimestampType());
        assertThat(timestampMapper).isInstanceOf(PrimitiveFieldMappers.TimestampMapper.class);

        // Test binary mapper
        CassandraFieldMapper binaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new BinaryType(10));
        assertThat(binaryMapper).isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);

        // Test varbinary mapper
        CassandraFieldMapper varbinaryMapper =
                CassandraFieldMapperFactory.createFieldMapper(new VarBinaryType(100));
        assertThat(varbinaryMapper).isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);
    }

    @Test
    void testDecimalMappers() {
        // Test regular decimal mapper (precision <= 18)
        CassandraFieldMapper decimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(10, 2));
        assertThat(decimalMapper).isInstanceOf(PrimitiveFieldMappers.DecimalMapper.class);

        // Test varint mapper (precision > 18)
        CassandraFieldMapper varintMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(25, 5));
        assertThat(varintMapper).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);

        // Test edge case: exactly 18 precision
        CassandraFieldMapper edgeDecimalMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(18, 2));
        assertThat(edgeDecimalMapper).isInstanceOf(PrimitiveFieldMappers.DecimalMapper.class);

        // Test edge case: 19 precision
        CassandraFieldMapper edgeVarintMapper =
                CassandraFieldMapperFactory.createFieldMapper(new DecimalType(19, 2));
        assertThat(edgeVarintMapper).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);
    }

    @Test
    void testArrayMapper() {
        // Test array of strings
        ArrayType stringArrayType = new ArrayType(VarCharType.STRING_TYPE);
        CassandraFieldMapper arrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(stringArrayType);
        assertThat(arrayMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);

        // Test nested array (array of array of integers)
        ArrayType nestedArrayType = new ArrayType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedArrayMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedArrayType);
        assertThat(nestedArrayMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
    }

    @Test
    void testMapMapper() {
        // Test map<string, integer>
        MapType mapType = new MapType(VarCharType.STRING_TYPE, new IntType());
        CassandraFieldMapper mapMapper = CassandraFieldMapperFactory.createFieldMapper(mapType);
        assertThat(mapMapper).isInstanceOf(CollectionFieldMappers.MapMapper.class);

        // Test nested map (map<string, map<string, integer>>)
        MapType nestedMapType =
                new MapType(
                        VarCharType.STRING_TYPE,
                        new MapType(VarCharType.STRING_TYPE, new IntType()));
        CassandraFieldMapper nestedMapMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMapType);
        assertThat(nestedMapMapper).isInstanceOf(CollectionFieldMappers.MapMapper.class);
    }

    @Test
    void testMultisetMapper() {
        // Test multiset<string> (Cassandra set)
        MultisetType multisetType = new MultisetType(VarCharType.STRING_TYPE);
        CassandraFieldMapper setMapper =
                CassandraFieldMapperFactory.createFieldMapper(multisetType);
        assertThat(setMapper).isInstanceOf(CollectionFieldMappers.SetMapper.class);

        // Test nested multiset
        MultisetType nestedMultisetType = new MultisetType(new ArrayType(new IntType()));
        CassandraFieldMapper nestedSetMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedMultisetType);
        assertThat(nestedSetMapper).isInstanceOf(CollectionFieldMappers.SetMapper.class);
    }

    @Test
    void testRowMapper() {
        // Test simple row type
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            VarCharType.STRING_TYPE, new IntType(), new BooleanType()
                        },
                        new String[] {"name", "age", "active"});
        CassandraFieldMapper rowMapper = CassandraFieldMapperFactory.createFieldMapper(rowType);
        assertThat(rowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);

        // Test nested row type
        RowType nestedRowType =
                RowType.of(
                        new LogicalType[] {
                            VarCharType.STRING_TYPE,
                            rowType, // nested row
                            new ArrayType(new IntType())
                        },
                        new String[] {"id", "user_info", "scores"});
        CassandraFieldMapper nestedRowMapper =
                CassandraFieldMapperFactory.createFieldMapper(nestedRowType);
        assertThat(nestedRowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);
    }

    @Test
    void testGenericMapperFallback() {
        CassandraFieldMapper stringMapper =
                CassandraFieldMapperFactory.createFieldMapper(VarCharType.STRING_TYPE);
        assertThat(stringMapper).isNotInstanceOf(PrimitiveFieldMappers.GenericMapper.class);
        assertThat(stringMapper).isInstanceOf(PrimitiveFieldMappers.StringMapper.class);
    }

    @Test
    void testSpecializedFactoryMethods() {
        // Test createVarintMapper
        CassandraFieldMapper varintMapper = CassandraFieldMapperFactory.createVarintMapper(30, 10);
        assertThat(varintMapper).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);

        // Test createInetMapper
        CassandraFieldMapper inetMapper = CassandraFieldMapperFactory.createInetMapper();
        assertThat(inetMapper).isInstanceOf(PrimitiveFieldMappers.InetMapper.class);

        // Test createBlobMapper
        CassandraFieldMapper blobMapper = CassandraFieldMapperFactory.createBlobMapper();
        assertThat(blobMapper).isInstanceOf(PrimitiveFieldMappers.BinaryMapper.class);

        // Test createDurationMapper
        CassandraFieldMapper durationMapper = CassandraFieldMapperFactory.createDurationMapper();
        assertThat(durationMapper).isInstanceOf(PrimitiveFieldMappers.DurationMapper.class);
    }

    @Test
    void testCreateTupleMapper() {
        // Test tuple mapper with mixed types
        List<LogicalType> elementTypes =
                Arrays.asList(
                        VarCharType.STRING_TYPE,
                        new IntType(),
                        new BooleanType(),
                        new DoubleType());

        CassandraFieldMapper tupleMapper =
                CassandraFieldMapperFactory.createTupleMapper(elementTypes);
        assertThat(tupleMapper).isInstanceOf(CollectionFieldMappers.TupleMapper.class);

        // Test empty tuple
        List<LogicalType> emptyTypes = Arrays.asList();
        CassandraFieldMapper emptyTupleMapper =
                CassandraFieldMapperFactory.createTupleMapper(emptyTypes);
        assertThat(emptyTupleMapper).isInstanceOf(CollectionFieldMappers.TupleMapper.class);

        // Test single element tuple
        List<LogicalType> singleTypes = Arrays.asList(VarCharType.STRING_TYPE);
        CassandraFieldMapper singleTupleMapper =
                CassandraFieldMapperFactory.createTupleMapper(singleTypes);
        assertThat(singleTupleMapper).isInstanceOf(CollectionFieldMappers.TupleMapper.class);
    }

    @Test
    void testComplexNestedStructures() {
        // Test deeply nested structure: array<map<string, row<name:string, scores:array<int>>>>
        RowType innerRowType =
                RowType.of(
                        new LogicalType[] {VarCharType.STRING_TYPE, new ArrayType(new IntType())},
                        new String[] {"name", "scores"});
        MapType mapType = new MapType(VarCharType.STRING_TYPE, innerRowType);
        ArrayType complexArrayType = new ArrayType(mapType);

        CassandraFieldMapper complexMapper =
                CassandraFieldMapperFactory.createFieldMapper(complexArrayType);
        assertThat(complexMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);

        // Test another complex structure: row<id:int, metadata:map<string,string>,
        // tags:multiset<string>>
        RowType complexRowType =
                RowType.of(
                        new LogicalType[] {
                            new IntType(),
                            new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                            new MultisetType(VarCharType.STRING_TYPE)
                        },
                        new String[] {"id", "metadata", "tags"});

        CassandraFieldMapper complexRowMapper =
                CassandraFieldMapperFactory.createFieldMapper(complexRowType);
        assertThat(complexRowMapper).isInstanceOf(CollectionFieldMappers.RowMapper.class);
    }

    @Test
    void testVarintMapperEdgeCases() {
        // Test various precision/scale combinations for varint (max precision is 38)
        CassandraFieldMapper varint1 = CassandraFieldMapperFactory.createVarintMapper(38, 0);
        assertThat(varint1).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);

        CassandraFieldMapper varint2 = CassandraFieldMapperFactory.createVarintMapper(38, 10);
        assertThat(varint2).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);

        // Test minimum values
        CassandraFieldMapper varint3 = CassandraFieldMapperFactory.createVarintMapper(1, 0);
        assertThat(varint3).isInstanceOf(PrimitiveFieldMappers.VarintMapper.class);
    }

    @Test
    void testRecursiveMapperCreation() {
        RowType deepRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new MultisetType(new DoubleType())},
                        new String[] {"id", "values"});
        MapType deepMapType = new MapType(VarCharType.STRING_TYPE, deepRowType);
        ArrayType deepArrayType = new ArrayType(new ArrayType(deepMapType));

        CassandraFieldMapper deepMapper =
                CassandraFieldMapperFactory.createFieldMapper(deepArrayType);
        assertThat(deepMapper).isInstanceOf(CollectionFieldMappers.ArrayMapper.class);
        assertThat(deepMapper).isNotNull();
    }
}
