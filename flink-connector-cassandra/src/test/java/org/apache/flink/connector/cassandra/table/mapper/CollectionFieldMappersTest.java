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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** Unit tests for {@link CollectionFieldMappers}. */
class CollectionFieldMappersTest {

    @Mock private Row mockRow;
    @Mock private TupleValue mockTupleValue;
    @Mock private UDTValue mockUDTValue;
    @Mock private CassandraFieldMapper mockElementMapper;
    @Mock private CassandraFieldMapper mockKeyMapper;
    @Mock private CassandraFieldMapper mockValueMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testArrayMapper() {
        CollectionFieldMappers.ArrayMapper mapper =
                new CollectionFieldMappers.ArrayMapper(mockElementMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test array value
        when(mockRow.isNull("field")).thenReturn(false);
        List<Object> testList = Arrays.asList("a", "b", "c");
        when(mockRow.getObject("field")).thenReturn(testList);
        when(mockElementMapper.convertValue("a")).thenReturn("converted_a");
        when(mockElementMapper.convertValue("b")).thenReturn("converted_b");
        when(mockElementMapper.convertValue("c")).thenReturn("converted_c");

        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(GenericArrayData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();

        GenericArrayData converted = (GenericArrayData) mapper.convertValue(testList);
        assertThat(converted.size()).isEqualTo(3);
        Object[] array = converted.toObjectArray();
        assertThat(array).containsExactly("converted_a", "converted_b", "converted_c");
    }

    @Test
    void testMapMapper() {
        CollectionFieldMappers.MapMapper mapper =
                new CollectionFieldMappers.MapMapper(mockKeyMapper, mockValueMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test map value
        when(mockRow.isNull("field")).thenReturn(false);
        Map<Object, Object> testMap = new HashMap<>();
        testMap.put("key1", 10);
        testMap.put("key2", 20);
        when(mockRow.getObject("field")).thenReturn(testMap);
        when(mockKeyMapper.convertValue("key1")).thenReturn("converted_key1");
        when(mockKeyMapper.convertValue("key2")).thenReturn("converted_key2");
        when(mockValueMapper.convertValue(10)).thenReturn(100);
        when(mockValueMapper.convertValue(20)).thenReturn(200);

        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(GenericMapData.class);
        assertThat(mapper.convertValue(null)).isNull();

        GenericMapData converted = (GenericMapData) mapper.convertValue(testMap);
        assertThat(converted.size()).isEqualTo(2);
    }

    @Test
    void testSetMapper() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test set value
        when(mockRow.isNull("field")).thenReturn(false);
        Set<Object> testSet = new HashSet<>(Arrays.asList("x", "y", "z"));
        when(mockRow.getObject("field")).thenReturn(testSet);
        when(mockElementMapper.convertValue("x")).thenReturn("converted_x");
        when(mockElementMapper.convertValue("y")).thenReturn("converted_y");
        when(mockElementMapper.convertValue("z")).thenReturn("converted_z");

        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(GenericArrayData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();

        GenericArrayData converted = (GenericArrayData) mapper.convertValue(testSet);
        assertThat(converted.size()).isEqualTo(3);
        // Note: Set order is not guaranteed, so we check that all converted values are present
        Object[] array = converted.toObjectArray();
        assertThat(array).containsExactlyInAnyOrder("converted_x", "converted_y", "converted_z");
    }

    @Test
    void testTupleMapper() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        CollectionFieldMappers.TupleMapper mapper =
                new CollectionFieldMappers.TupleMapper(fieldMappers);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test tuple value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getObject("field")).thenReturn(mockTupleValue);
        when(mockTupleValue.isNull(0)).thenReturn(false);
        when(mockTupleValue.isNull(1)).thenReturn(false);
        when(mockTupleValue.getObject(0)).thenReturn("tuple_field1");
        when(mockTupleValue.getObject(1)).thenReturn(42);
        when(mockElementMapper.convertValue("tuple_field1")).thenReturn("converted_tuple_field1");
        when(mockValueMapper.convertValue(42)).thenReturn(420);

        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(GenericRowData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();

        GenericRowData converted = (GenericRowData) mapper.convertValue(mockTupleValue);
        assertThat(converted.getArity()).isEqualTo(2);
        assertThat(converted.getField(0)).isEqualTo("converted_tuple_field1");
        assertThat(converted.getField(1)).isEqualTo(420);
    }

    @Test
    void testTupleMapperWithNullFields() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        CollectionFieldMappers.TupleMapper mapper =
                new CollectionFieldMappers.TupleMapper(fieldMappers);

        // Test tuple with null fields
        when(mockTupleValue.isNull(0)).thenReturn(true);
        when(mockTupleValue.isNull(1)).thenReturn(false);
        when(mockTupleValue.getObject(1)).thenReturn(42);
        when(mockValueMapper.convertValue(42)).thenReturn(420);

        GenericRowData converted = (GenericRowData) mapper.convertValue(mockTupleValue);
        assertThat(converted.getArity()).isEqualTo(2);
        assertThat(converted.isNullAt(0)).isTrue();
        assertThat(converted.getField(1)).isEqualTo(420);
    }

    @Test
    void testRowMapper() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        String[] fieldNames = {"name", "age"};
        CollectionFieldMappers.RowMapper mapper =
                new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

        // Test null value
        when(mockRow.isNull("field")).thenReturn(true);
        assertThat(mapper.extractFromRow(mockRow, "field")).isNull();

        // Test UDT value
        when(mockRow.isNull("field")).thenReturn(false);
        when(mockRow.getObject("field")).thenReturn(mockUDTValue);
        when(mockUDTValue.isNull("name")).thenReturn(false);
        when(mockUDTValue.isNull("age")).thenReturn(false);
        when(mockUDTValue.getObject("name")).thenReturn("John");
        when(mockUDTValue.getObject("age")).thenReturn(30);
        when(mockElementMapper.convertValue("John")).thenReturn("converted_John");
        when(mockValueMapper.convertValue(30)).thenReturn(300);

        Object result = mapper.extractFromRow(mockRow, "field");
        assertThat(result).isInstanceOf(GenericRowData.class);

        // Test convertValue method
        assertThat(mapper.convertValue(null)).isNull();

        GenericRowData converted = (GenericRowData) mapper.convertValue(mockUDTValue);
        assertThat(converted.getArity()).isEqualTo(2);
        assertThat(converted.getField(0)).isEqualTo("converted_John");
        assertThat(converted.getField(1)).isEqualTo(300);
    }

    @Test
    void testRowMapperWithNullFields() {
        CassandraFieldMapper[] fieldMappers = {mockElementMapper, mockValueMapper};
        String[] fieldNames = {"name", "age"};
        CollectionFieldMappers.RowMapper mapper =
                new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);

        // Test UDT with null fields
        when(mockUDTValue.isNull("name")).thenReturn(true);
        when(mockUDTValue.isNull("age")).thenReturn(false);
        when(mockUDTValue.getObject("age")).thenReturn(30);
        when(mockValueMapper.convertValue(30)).thenReturn(300);

        GenericRowData converted = (GenericRowData) mapper.convertValue(mockUDTValue);
        assertThat(converted.getArity()).isEqualTo(2);
        assertThat(converted.isNullAt(0)).isTrue();
        assertThat(converted.getField(1)).isEqualTo(300);
    }

    @Test
    void testArrayMapperWithEmptyList() {
        CollectionFieldMappers.ArrayMapper mapper =
                new CollectionFieldMappers.ArrayMapper(mockElementMapper);

        List<Object> emptyList = Arrays.asList();
        GenericArrayData converted = (GenericArrayData) mapper.convertValue(emptyList);
        assertThat(converted.size()).isEqualTo(0);
    }

    @Test
    void testMapMapperWithEmptyMap() {
        CollectionFieldMappers.MapMapper mapper =
                new CollectionFieldMappers.MapMapper(mockKeyMapper, mockValueMapper);

        Map<Object, Object> emptyMap = new HashMap<>();
        GenericMapData converted = (GenericMapData) mapper.convertValue(emptyMap);
        assertThat(converted.size()).isEqualTo(0);
    }

    @Test
    void testSetMapperWithEmptySet() {
        CollectionFieldMappers.SetMapper mapper =
                new CollectionFieldMappers.SetMapper(mockElementMapper);

        Set<Object> emptySet = new HashSet<>();
        GenericArrayData converted = (GenericArrayData) mapper.convertValue(emptySet);
        assertThat(converted.size()).isEqualTo(0);
    }
}
