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

public class SnowflakeQueryFactory implements QueryFactory {
  private JdbcClientPool connectionPool;

  SnowflakeQueryFactory(JdbcClientPool conn) {
    connectionPool = conn;
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    String query = "show schemas";

    QueryRunner run = new QueryRunner(true);
    List<SnowflakeSchema> schemas = null;
    try {
      if (namespace == null || namespace.isEmpty()) {
        query += " in account";
        final String finalQuery = query;
        schemas =
            connectionPool.run(
                conn -> {
                  return run.query(conn, finalQuery, SnowflakeSchema.createHandler());
                });
      } else {
        query += " in identifier(?)";
        final String finalQuery = query;
        schemas =
            connectionPool.run(
                conn -> {
                  return run.query(
                      conn,
                      finalQuery,
                      SnowflakeSchema.createHandler(),
                      namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1));
                });
      }

    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to list schemas for namespace %s", namespace.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Connection error while listing schemas");
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {

    String query = "show iceberg tables";

    QueryRunner run = new QueryRunner(true);

    List<SnowflakeTable> tables = Lists.newArrayList();
    try {
      if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
        query += " in identifier(?)";
        final String finalQuery = query;
        tables.addAll(
            connectionPool.run(
                conn -> {
                  return run.query(
                      conn,
                      finalQuery,
                      SnowflakeTable.createHandler(),
                      String.format(
                          "%s.%s",
                          namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                          namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1)));
                }));
      } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
        query += " in account";
        final String finalQuery = query;
        tables.addAll(
            connectionPool.run(
                conn -> {
                  return run.query(conn, finalQuery, SnowflakeTable.createHandler());
                }));
        tables.removeIf(
            table ->
                !table
                    .getDatabase()
                    .equals(namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      } else {
        query += " in account";
        final String finalQuery = query;
        tables.addAll(
            connectionPool.run(
                conn -> {
                  return run.query(conn, finalQuery, SnowflakeTable.createHandler());
                }));
      }

    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to list tables for namespace %s", namespace.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Connection error while listing tables");
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    QueryRunner run = new QueryRunner(true);

    SnowflakeTableMetadata tableMeta = null;
    try {
      final String finalQuery = "select SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) as location";
      tableMeta =
          connectionPool.run(
              conn -> {
                return run.query(
                    conn,
                    finalQuery,
                    SnowflakeTableMetadata.createHandler(),
                    tableIdentifier.toString());
              });
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to get table metadata for %s", tableIdentifier.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Connection error while getting table metadata");
    }
    return tableMeta;
  }

  @Override
  public void close() {
    connectionPool.close();
  }
}
