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
package org.apache.iceberg.snowflake;

import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeQueryFactory implements QueryFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeQueryFactory.class);
  private final JdbcClientPool connectionPool;

  SnowflakeQueryFactory(JdbcClientPool conn) {
    connectionPool = conn;
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    String baseQuery = "show schemas";

    QueryRunner run = new QueryRunner(true);
    List<SnowflakeSchema> schemas;
    try {
      // for empty or null namespace search for all schemas at account level where the user
      // has access to list.
      if (namespace == null || namespace.isEmpty()) {
        baseQuery += " in account";
        final String finalQuery = baseQuery;
        schemas =
            connectionPool.run(
                conn -> run.query(conn, finalQuery, SnowflakeSchema.createHandler()));
      }
      // otherwise restrict listing of schema within the database.
      else {
        baseQuery += " in identifier(?)";
        final String finalQuery = baseQuery;
        schemas =
            connectionPool.run(
                conn ->
                    run.query(
                        conn,
                        finalQuery,
                        SnowflakeSchema.createHandler(),
                        namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      }

    } catch (SQLException e) {
      LOG.error(
          "Failed to list schemas for namespace {}",
          namespace != null ? namespace.toString() : "",
          e);
      throw new UncheckedSQLException(
          e,
          "Failed to list schemas for namespace %s",
          namespace != null ? namespace.toString() : "");
    } catch (InterruptedException e) {
      LOG.error(
          "Failed to list schemas for namespace {}",
          namespace != null ? namespace.toString() : "",
          e);
      throw new UncheckedInterruptedException(e, "Connection error while listing schemas");
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {

    String baseQuery = "show iceberg tables";

    QueryRunner run = new QueryRunner(true);

    List<SnowflakeTable> tables = Lists.newArrayList();
    try {
      // For two level namespace, search for iceberg tables within the given schema.
      if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
        baseQuery += " in identifier(?)";
        final String finalQuery = baseQuery;
        tables.addAll(
            connectionPool.run(
                conn ->
                    run.query(
                        conn,
                        finalQuery,
                        SnowflakeTable.createHandler(),
                        String.format(
                            "%s.%s",
                            namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                            namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1)))));
      } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
        final String finalQuery = baseQuery + " in database identifier(?)";
        tables.addAll(
            connectionPool.run(
                conn ->
                    run.query(
                        conn,
                        finalQuery,
                        SnowflakeTable.createHandler(),
                        namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1))));
      }
      // For empty or db level namespace, search at account level. For Db level namespace results
      // would be filtered to exclude tables from other databases.
      else {
        baseQuery += " in account";
        final String finalQuery = baseQuery;
        tables.addAll(
            connectionPool.run(
                conn -> run.query(conn, finalQuery, SnowflakeTable.createHandler())));
      }

      // In case of DB level namespace, filter the results to given namespace
      if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
        tables.removeIf(
            table ->
                !table
                    .getDatabase()
                    .equalsIgnoreCase(namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      }

    } catch (SQLException e) {
      LOG.error("Failed to list tables for namespace {}", namespace, e);
      throw new UncheckedSQLException(
          e, "Failed to list tables for namespace %s", namespace.toString());
    } catch (InterruptedException e) {
      LOG.error("Failed to list tables for namespace {}", namespace, e);
      throw new UncheckedInterruptedException(e, "Connection error while listing tables");
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    QueryRunner run = new QueryRunner(true);

    SnowflakeTableMetadata tableMeta;
    try {
      final String finalQuery = "select SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) as location";
      tableMeta =
          connectionPool.run(
              conn ->
                  run.query(
                      conn,
                      finalQuery,
                      SnowflakeTableMetadata.createHandler(),
                      tableIdentifier.toString()));
    } catch (SQLException e) {
      LOG.error("Failed to get metadata for table {}", tableIdentifier.toString(), e);
      throw new UncheckedSQLException(
          e, "Failed to get table metadata for %s", tableIdentifier.toString());
    } catch (InterruptedException e) {
      LOG.error("Failed to get metadata for table {}", tableIdentifier.toString(), e);
      throw new UncheckedInterruptedException(e, "Connection error while getting table metadata");
    }
    return tableMeta;
  }

  @Override
  public void close() {
    connectionPool.close();
  }
}
