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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/** Factory for creating appropriate field mappers based on Flink logical types. */
@Internal
public final class CassandraFieldMapperFactory {

    private CassandraFieldMapperFactory() {}

    /**
     * Creates a field mapper for the given logical type.
     *
     * @param logicalType the Flink logical type
     * @return appropriate field mapper for the type
     */
    public static CassandraFieldMapper createFieldMapper(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

        switch (typeRoot) {
            case BOOLEAN:
                return new PrimitiveFieldMappers.BooleanMapper();
            case TINYINT:
                return new PrimitiveFieldMappers.ByteMapper();
            case SMALLINT:
                return new PrimitiveFieldMappers.ShortMapper();
            case INTEGER:
                return new PrimitiveFieldMappers.IntegerMapper();
            case BIGINT:
                return new PrimitiveFieldMappers.LongMapper();
            case FLOAT:
                return new PrimitiveFieldMappers.FloatMapper();
            case DOUBLE:
                return new PrimitiveFieldMappers.DoubleMapper();
            case VARCHAR:
                // VARCHAR can be used for various Cassandra types: text, inet, timeuuid, etc.
                return new PrimitiveFieldMappers.StringMapper();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                // Check if this might be used for varint mapping - use VarintMapper for large
                // precision
                if (decimalType.getPrecision() > 18) {
                    return new PrimitiveFieldMappers.VarintMapper(decimalType);
                }
                return new PrimitiveFieldMappers.DecimalMapper(decimalType);
            case DATE:
                return new PrimitiveFieldMappers.DateMapper();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new PrimitiveFieldMappers.TimestampMapper();
            case BINARY:
            case VARBINARY:
                return new PrimitiveFieldMappers.BinaryMapper();
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                CassandraFieldMapper elementMapper = createFieldMapper(arrayType.getElementType());
                return new CollectionFieldMappers.ArrayMapper(elementMapper);
            case MAP:
                MapType mapType = (MapType) logicalType;
                CassandraFieldMapper keyMapper = createFieldMapper(mapType.getKeyType());
                CassandraFieldMapper valueMapper = createFieldMapper(mapType.getValueType());
                return new CollectionFieldMappers.MapMapper(keyMapper, valueMapper);
            case MULTISET:
                MultisetType multisetType = (MultisetType) logicalType;
                CassandraFieldMapper setElementMapper =
                        createFieldMapper(multisetType.getElementType());
                return new CollectionFieldMappers.SetMapper(setElementMapper);
            case ROW:
                RowType rowType = (RowType) logicalType;
                CassandraFieldMapper[] fieldMappers =
                        new CassandraFieldMapper[rowType.getFieldCount()];
                String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    fieldMappers[i] = createFieldMapper(rowType.getTypeAt(i));
                }

                return new CollectionFieldMappers.RowMapper(fieldMappers, fieldNames);
            default:
                return new PrimitiveFieldMappers.GenericMapper();
        }
    }

    /**
     * Creates a field mapper specifically for Cassandra varint type.
     *
     * @param precision the decimal precision for varint conversion
     * @param scale the decimal scale for varint conversion
     * @return varint field mapper
     */
    public static CassandraFieldMapper createVarintMapper(int precision, int scale) {
        return new PrimitiveFieldMappers.VarintMapper(new DecimalType(precision, scale));
    }

    /**
     * Creates a field mapper specifically for Cassandra inet type.
     *
     * @return inet field mapper that converts to string representation
     */
    public static CassandraFieldMapper createInetMapper() {
        return new PrimitiveFieldMappers.InetMapper();
    }

    /**
     * Creates a field mapper specifically for Cassandra blob type.
     *
     * @return binary field mapper
     */
    public static CassandraFieldMapper createBlobMapper() {
        return new PrimitiveFieldMappers.BinaryMapper();
    }

    /**
     * Creates a field mapper specifically for Cassandra tuple type.
     *
     * @param elementTypes the logical types of tuple elements
     * @return tuple field mapper
     */
    public static CassandraFieldMapper createTupleMapper(java.util.List<LogicalType> elementTypes) {
        CassandraFieldMapper[] fieldMappers = new CassandraFieldMapper[elementTypes.size()];
        for (int i = 0; i < elementTypes.size(); i++) {
            fieldMappers[i] = createFieldMapper(elementTypes.get(i));
        }
        return new CollectionFieldMappers.TupleMapper(fieldMappers);
    }

    /**
     * Creates a field mapper specifically for Cassandra duration type.
     *
     * @return duration field mapper that converts to string representation
     */
    public static CassandraFieldMapper createDurationMapper() {
        return new PrimitiveFieldMappers.DurationMapper();
    }
}
