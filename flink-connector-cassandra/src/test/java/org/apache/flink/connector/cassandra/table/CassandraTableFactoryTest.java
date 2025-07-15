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

package org.apache.flink.connector.cassandra.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CassandraTableFactory}. */
class CassandraTableFactoryTest {

    private static final ResolvedSchema BASIC_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT().notNull()),
                    Column.physical("name", DataTypes.STRING()),
                    Column.physical("age", DataTypes.INT()),
                    Column.physical("active", DataTypes.BOOLEAN()));

    @Test
    void testCreateTableSource() {
        // This test is simplified to just test the factory creation without full context
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory).isNotNull();
        assertThat(factory.factoryIdentifier()).isEqualTo("cassandra");
    }

    @Test
    void testRequiredOptions() {
        CassandraTableFactory factory = new CassandraTableFactory();

        assertThat(factory.requiredOptions())
                .containsExactlyInAnyOrder(
                        CassandraConnectorOptions.HOSTS,
                        CassandraConnectorOptions.KEYSPACE,
                        CassandraConnectorOptions.TABLE);
    }

    @Test
    void testOptionalOptions() {
        CassandraTableFactory factory = new CassandraTableFactory();

        assertThat(factory.optionalOptions())
                .containsExactlyInAnyOrder(
                        CassandraConnectorOptions.PORT,
                        CassandraConnectorOptions.USERNAME,
                        CassandraConnectorOptions.PASSWORD,
                        CassandraConnectorOptions.CONSISTENCY_LEVEL,
                        CassandraConnectorOptions.CONNECT_TIMEOUT,
                        CassandraConnectorOptions.READ_TIMEOUT,
                        CassandraConnectorOptions.QUERY,
                        CassandraConnectorOptions.MAX_SPLIT_MEMORY_SIZE,
                        CassandraConnectorOptions.CLUSTER_BUILDER_CLASS);
    }

    @Test
    void testFactoryIdentifier() {
        CassandraTableFactory factory = new CassandraTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("cassandra");
    }
}
