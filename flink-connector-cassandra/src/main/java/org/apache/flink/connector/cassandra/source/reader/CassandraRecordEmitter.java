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

package org.apache.flink.connector.cassandra.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.cassandra.source.reader.converter.CassandraRowToTypeConverter;
import org.apache.flink.connector.cassandra.source.split.CassandraSplit;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * {@link RecordEmitter} that uses a mapper function to convert {@link ResultSet} directly to the
 * desired output type (existing behavior - deprecated).
 *
 * @param <OUT> type of record to output
 * @deprecated Use {@link CassandraRowEmitter} with {@link CassandraRowToTypeConverter} instead.
 */
@Deprecated
class CassandraRecordEmitter<OUT> implements RecordEmitter<CassandraRow, OUT, CassandraSplit> {

    private final Function<ResultSet, OUT> mapper;

    public CassandraRecordEmitter(Function<ResultSet, OUT> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void emitRecord(
            CassandraRow cassandraRow, SourceOutput<OUT> output, CassandraSplit cassandraSplit) {
        // Create fake ResultSet from Row for backward compatibility
        OUT record = mapper.apply(new SingleRowResultSet(cassandraRow.getRow()));
        if (record != null) {
            output.collect(record);
        }
    }

    /**
     * A ResultSet implementation that wraps a single Row. This is needed because the old POJO-based
     * API expects a ResultSet.
     */
    private static class SingleRowResultSet implements ResultSet {
        private final Row row;
        private boolean consumed = false;

        SingleRowResultSet(Row row) {
            this.row = row;
        }

        @Override
        public Row one() {
            if (!consumed) {
                consumed = true;
                return row;
            }
            return null;
        }

        @Override
        public List<Row> all() {
            if (!consumed) {
                consumed = true;
                return singletonList(row);
            }
            return singletonList(null);
        }

        @Override
        public Iterator<Row> iterator() {
            return all().iterator();
        }

        @Override
        public boolean isExhausted() {
            return consumed;
        }

        @Override
        public boolean isFullyFetched() {
            return true;
        }

        @Override
        public int getAvailableWithoutFetching() {
            return consumed ? 0 : 1;
        }

        @Override
        public ListenableFuture<ResultSet> fetchMoreResults() {
            return null;
        }

        @Override
        public ExecutionInfo getExecutionInfo() {
            return null;
        }

        @Override
        public List<ExecutionInfo> getAllExecutionInfo() {
            return singletonList(getExecutionInfo());
        }

        @Override
        public boolean wasApplied() {
            return true;
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return row.getColumnDefinitions();
        }
    }
}
