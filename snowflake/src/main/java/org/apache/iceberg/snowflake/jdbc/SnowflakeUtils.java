/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.snowflake.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

final class SnowflakeUtils {
  static final String DEFAULT_CATALOG_NAME = "snowlog";
  static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";

  static final String ENABLE_ICEBERG_AT_SESSION_LEVEL =
      "alter session set ENABLE_ICEBERG_TABLES=true";
  static final String METADATA_LOCATION = "metadataLocation";
  static final String METADATA_LOCATION_QUERY = "select SYSTEM$GET_ICEBERG_TABLE_INFORMATION('%s')";

  static final String LIST_TABLES_QUERY = "show iceberg tables";

  static final String LIST_TABLES_DB_NAME_COL = "database_name";

  static final String LIST_TABLES_SCHEMA_NAME_COL = "schema_name";

  static final String LIST_TABLES_TABLE_NAME_COL = "name";

  static final String LIST_NAMESPACE_QUERY = "show schemas";

  static final String LIST_NAMESPACE_DB_NAME_COL = "database_name";

  static final String LIST_NAMESPACE_SCHEMA_NAME_COL = "name";

  static final String LIST_NAMESPACE_ACCOUNT_SCOPE = "in account";

  static final String LIST_NAMESPACE_DB_SCOPE = "in database";

  static final int MAX_NAMESPACE_DEPTH = 2;

  static final int NAMESPACE_DB_LEVEL = 1;

  static final int NAMESPACE_SCHEMA_LEVEL = 2;

  static final String RENAME_TABLE_QUERY = "ALTER TABLE %s RENAME TO %s";

  static final String DROP_TABLE_QUERY = "DROP TABLE %s";

  static final String CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS %s";

  static final String CREATE_SCHEMA_QUERY = "CREATE SCHEMA IF NOT EXISTS %s";

  static final String DROP_DATABASE_QUERY = "DROP DATABASE IF EXISTS %s";

  static final String DROP_SCHEMA_QUERY = "DROP SCHEMA IF EXISTS %s";

  private SnowflakeUtils() {}

  @FunctionalInterface
  interface RowProducer<R> {
    R apply(ResultSet result) throws SQLException;
  }

  @SuppressWarnings("checkstyle:NestedTryDepth")
  public static <R> List<R> fetch(
      JdbcClientPool connections, RowProducer<R> toRow, String sql, String... args) {
    try {
      return connections.run(
          conn -> {
            List<R> result = Lists.newArrayList();

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                preparedStatement.setString(pos + 1, args[pos]);
              }

              try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                  result.add(toRow.apply(rs));
                }
              }
            }

            return result;
          });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to execute query: %s", sql);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
    }
  }
}
