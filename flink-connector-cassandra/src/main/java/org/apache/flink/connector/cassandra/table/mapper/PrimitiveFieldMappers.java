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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;

import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;

/** Collection of primitive type field mappers for Cassandra to Flink conversion. */
@Internal
public final class PrimitiveFieldMappers {

    private PrimitiveFieldMappers() {}

    /** Boolean field mapper. */
    public static final class BooleanMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getBool(fieldName);
        }
    }

    /** Byte field mapper. */
    public static final class ByteMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getByte(fieldName);
        }
    }

    /** Short field mapper. */
    public static final class ShortMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getShort(fieldName);
        }
    }

    /** Integer field mapper. */
    public static final class IntegerMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getInt(fieldName);
        }
    }

    /** Long field mapper. */
    public static final class LongMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getLong(fieldName);
        }
    }

    /** Float field mapper. */
    public static final class FloatMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getFloat(fieldName);
        }
    }

    /** Double field mapper. */
    public static final class DoubleMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getDouble(fieldName);
        }
    }

    /** String field mapper that handles text, uuid, and timeuuid types. */
    public static final class StringMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            // Use getString() which handles text, uuid, and timeuuid uniformly
            return convertValue(row.getString(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            return StringData.fromString(value.toString());
        }
    }

    /** Decimal field mapper with precision and scale handling. */
    public static final class DecimalMapper implements CassandraFieldMapper {
        private final DecimalType decimalType;

        public DecimalMapper(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getDecimal(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            BigDecimal decimal = (BigDecimal) value;
            return DecimalData.fromBigDecimal(
                    decimal, decimalType.getPrecision(), decimalType.getScale());
        }
    }

    /** Date field mapper that handles Cassandra LocalDate to Flink internal date format. */
    public static final class DateMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getDate(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            LocalDate date = (LocalDate) value;
            return (int)
                    java.time.LocalDate.of(date.getYear(), date.getMonth(), date.getDay())
                            .toEpochDay();
        }
    }

    /** Timestamp field mapper that handles various timestamp formats. */
    public static final class TimestampMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getTimestamp(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Date) {
                return TimestampData.fromInstant(((Date) value).toInstant());
            } else if (value instanceof Instant) {
                return TimestampData.fromInstant((Instant) value);
            }
            return value;
        }
    }

    /** Binary field mapper that handles Cassandra blob types. */
    public static final class BinaryMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getBytes(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof ByteBuffer) {
                ByteBuffer buffer = (ByteBuffer) value;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                return bytes;
            }
            return (byte[]) value;
        }
    }

    /** Varint field mapper that handles Cassandra varint (BigInteger) types. */
    public static final class VarintMapper implements CassandraFieldMapper {
        private final DecimalType decimalType;

        public VarintMapper(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getVarint(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            BigInteger varint = (BigInteger) value;
            return DecimalData.fromBigDecimal(
                    new BigDecimal(varint), decimalType.getPrecision(), decimalType.getScale());
        }
    }

    /** Inet field mapper that handles Cassandra inet (InetAddress) types. */
    public static final class InetMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.getInet(fieldName));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            InetAddress inet = (InetAddress) value;
            return StringData.fromString(inet.getHostAddress());
        }
    }

    /** Duration field mapper that handles Cassandra duration types. */
    public static final class DurationMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            if (row.isNull(fieldName)) {
                return null;
            }
            return convertValue(row.get(fieldName, Duration.class));
        }

        @Override
        public Object convertValue(Object value) {
            if (value == null) {
                return null;
            }
            Duration duration = (Duration) value;
            return StringData.fromString(duration.toString());
        }
    }

    /** Generic field mapper for unsupported types. */
    public static final class GenericMapper implements CassandraFieldMapper {
        @Override
        public Object extractFromRow(Row row, String fieldName) {
            return row.isNull(fieldName) ? null : row.getObject(fieldName);
        }
    }
}
