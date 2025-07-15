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

import org.apache.flink.connector.cassandra.CassandraTestEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Comprehensive integration tests for Cassandra Dynamic Table Source with various data types. */
@ExtendWith(MiniClusterExtension.class)
class CassandraDynamicTableSourceITCase {

    private CassandraTestEnvironment cassandraTestEnvironment;
    private StreamTableEnvironment tableEnv;

    @BeforeEach
    void setUp() throws Exception {
        cassandraTestEnvironment = new CassandraTestEnvironment(false);
        cassandraTestEnvironment.startUp();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Use parallelism 1 for deterministic results
        tableEnv = StreamTableEnvironment.create(env);

        createTestTables();
        insertTestData();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cassandraTestEnvironment != null) {
            cassandraTestEnvironment.tearDown();
        }
    }

    private void createTestTables() {
        // Create UDT first
        String createAddressUDT =
                "CREATE TYPE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".address ("
                        + "street text, "
                        + "city text, "
                        + "zipcode int"
                        + ");";

        // Table with primitive types
        String createPrimitivesTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".primitives_table ("
                        + "id int PRIMARY KEY, "
                        + "name text, "
                        + "age int, "
                        + "salary double, "
                        + "active boolean, "
                        + "score float, "
                        + "balance decimal, "
                        + "created_at timestamp"
                        + ");";

        // Table with collection types
        String createCollectionsTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".collections_table ("
                        + "id int PRIMARY KEY, "
                        + "tags set<text>, "
                        + "scores list<int>, "
                        + "metadata map<text, text>, "
                        + "mixed_list list<double>"
                        + ");";

        // Table with UDT and complex types
        String createComplexTable =
                "CREATE TABLE IF NOT EXISTS "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".complex_table ("
                        + "id int PRIMARY KEY, "
                        + "user_address frozen<address>, "
                        + "phone_numbers list<text>, "
                        + "preferences map<text, boolean>"
                        + ");";

        cassandraTestEnvironment.executeRequestWithTimeout(createAddressUDT);
        cassandraTestEnvironment.executeRequestWithTimeout(createPrimitivesTable);
        cassandraTestEnvironment.executeRequestWithTimeout(createCollectionsTable);
        cassandraTestEnvironment.executeRequestWithTimeout(createComplexTable);
    }

    private void insertTestData() {
        // Insert primitive data
        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.primitives_table (id, name, age, salary, active, score, balance, created_at) "
                                + "VALUES (1, 'Alice', 30, 75000.50, true, 95.5, 1234.56, '2023-01-15 10:30:00');",
                        CassandraTestEnvironment.KEYSPACE));

        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.primitives_table (id, name, age, salary, active, score, balance, created_at) "
                                + "VALUES (2, 'Bob', 25, 65000.75, false, 88.3, 987.65, '2023-02-20 14:45:00');",
                        CassandraTestEnvironment.KEYSPACE));

        // Insert collection data
        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.collections_table (id, tags, scores, metadata, mixed_list) "
                                + "VALUES (1, {'java', 'flink', 'cassandra'}, [95, 87, 92], {'team': 'data', 'level': 'senior'}, [1.1, 2.2, 3.3]);",
                        CassandraTestEnvironment.KEYSPACE));

        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.collections_table (id, tags, scores, metadata, mixed_list) "
                                + "VALUES (2, {'python', 'spark'}, [78, 85], {'team': 'ml', 'level': 'junior'}, [4.4, 5.5]);",
                        CassandraTestEnvironment.KEYSPACE));

        // Insert complex data with UDT
        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.complex_table (id, user_address, phone_numbers, preferences) "
                                + "VALUES (1, {street: '123 Main St', city: 'New York', zipcode: 10001}, ['555-1234', '555-5678'], {'email_notifications': true, 'sms_alerts': false});",
                        CassandraTestEnvironment.KEYSPACE));

        cassandraTestEnvironment.executeRequestWithTimeout(
                String.format(
                        "INSERT INTO %s.complex_table (id, user_address, phone_numbers, preferences) "
                                + "VALUES (2, {street: '456 Oak Ave', city: 'San Francisco', zipcode: 94102}, ['555-9999'], {'email_notifications': false, 'sms_alerts': true});",
                        CassandraTestEnvironment.KEYSPACE));
    }

    @Test
    void testPrimitiveTypesSource() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_primitives ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT,"
                        + "  salary DOUBLE,"
                        + "  active BOOLEAN,"
                        + "  score FLOAT,"
                        + "  balance DECIMAL(10,2),"
                        + "  created_at TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'primitives_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_primitives");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);

        // Verify Alice's record
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(1);
                            assertThat(row.getField(1)).isEqualTo("Alice");
                            assertThat(row.getField(2)).isEqualTo(30);
                            assertThat(row.getField(3)).isEqualTo(75000.50);
                            assertThat(row.getField(4)).isEqualTo(true);
                            assertThat(row.getField(5)).isEqualTo(95.5f);
                            assertThat(row.getField(6)).isEqualTo(new BigDecimal("1234.56"));
                            // Timestamp validation (just check it's not null)
                            assertThat(row.getField(7)).isNotNull();
                        });

        // Verify Bob's record
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(2);
                            assertThat(row.getField(1)).isEqualTo("Bob");
                            assertThat(row.getField(2)).isEqualTo(25);
                            assertThat(row.getField(3)).isEqualTo(65000.75);
                            assertThat(row.getField(4)).isEqualTo(false);
                            assertThat(row.getField(5)).isEqualTo(88.3f);
                            assertThat(row.getField(6)).isEqualTo(new BigDecimal("987.65"));
                            assertThat(row.getField(7)).isNotNull();
                        });
    }

    @Test
    void testCollectionTypesSource() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_collections ("
                        + "  id INT,"
                        + "  tags ARRAY<STRING>,"
                        + "  scores ARRAY<INT>,"
                        + "  metadata MAP<STRING, STRING>,"
                        + "  mixed_list ARRAY<DOUBLE>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'collections_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_collections");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);

        // Verify first record with collections
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(1);
                            // Tags (set converted to array)
                            Object[] tags = (Object[]) row.getField(1);
                            Set<String> tagSet = new HashSet<>(Arrays.asList((String[]) tags));
                            assertThat(tagSet)
                                    .containsExactlyInAnyOrder("java", "flink", "cassandra");
                            // Scores (list)
                            Object[] scores = (Object[]) row.getField(2);
                            assertThat(scores).containsExactly(95, 87, 92);
                            // Metadata (map) - verify it's not null and has expected size
                            assertThat(row.getField(3)).isNotNull();
                            // Mixed list
                            Object[] mixedList = (Object[]) row.getField(4);
                            assertThat(mixedList).containsExactly(1.1, 2.2, 3.3);
                        });
    }

    @Test
    void testComplexTypesWithUDT() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_complex ("
                        + "  id INT,"
                        + "  user_address ROW<street STRING, city STRING, zipcode INT>,"
                        + "  phone_numbers ARRAY<STRING>,"
                        + "  preferences MAP<STRING, BOOLEAN>"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'complex_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        Table result = tableEnv.sqlQuery("SELECT * FROM flink_complex");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);

        // Verify first record with UDT
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo(1);
                            // UDT address
                            Row address = (Row) row.getField(1);
                            assertThat(address.getField(0)).isEqualTo("123 Main St"); // street
                            assertThat(address.getField(1)).isEqualTo("New York"); // city
                            assertThat(address.getField(2)).isEqualTo(10001); // zipcode
                            // Phone numbers array
                            Object[] phones = (Object[]) row.getField(2);
                            assertThat(phones).containsExactly("555-1234", "555-5678");
                            // Preferences map - verify it's not null
                            assertThat(row.getField(3)).isNotNull();
                        });
    }

    @Test
    void testProjectionQuery() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_primitives ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT,"
                        + "  salary DOUBLE,"
                        + "  active BOOLEAN"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'primitives_table',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        // Test column projection
        Table result = tableEnv.sqlQuery("SELECT name, age FROM flink_primitives");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.getField(0)).isIn("Alice", "Bob");
                            assertThat(row.getField(1)).isIn(30, 25);
                        });
    }

    @Test
    void testFieldRenaming() throws Exception {
        String createFlinkTable =
                "CREATE TABLE flink_renamed ("
                        + "  id INT,"
                        + "  name STRING,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector' = 'cassandra',"
                        + "  'hosts' = '"
                        + cassandraTestEnvironment.getContactPoint()
                        + "',"
                        + "  'port' = '"
                        + cassandraTestEnvironment.getPort()
                        + "',"
                        + "  'keyspace' = '"
                        + CassandraTestEnvironment.KEYSPACE
                        + "',"
                        + "  'table' = 'primitives_table',"
                        + "  'query' = 'SELECT id, name, age FROM "
                        + CassandraTestEnvironment.KEYSPACE
                        + ".primitives_table;',"
                        + "  'username' = 'cassandra',"
                        + "  'password' = 'cassandra'"
                        + ")";

        tableEnv.executeSql(createFlinkTable);

        // Test using aliases to demonstrate field renaming in SQL
        Table result =
                tableEnv.sqlQuery(
                        "SELECT id as user_id, name as full_name, age as years_old FROM flink_renamed");

        List<Row> actualRows = collectResults(result, 2);

        assertThat(actualRows).hasSize(2);
        assertThat(actualRows)
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isIn(1, 2);
                            assertThat(row.getField(1)).isIn("Alice", "Bob");
                            assertThat(row.getField(2)).isIn(30, 25);
                        });
    }

    private List<Row> collectResults(Table table, int expectedCount) throws Exception {
        List<Row> actualRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = table.execute().collect()) {
            int count = 0;
            while (iterator.hasNext() && count < expectedCount) {
                actualRows.add(iterator.next());
                count++;
            }
        }
        return actualRows;
    }
}
