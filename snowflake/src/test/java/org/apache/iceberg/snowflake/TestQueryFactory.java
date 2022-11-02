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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;

public class TestQueryFactory implements QueryFactory {
  private final JdbcClientPool connectionPool;

  TestQueryFactory(JdbcClientPool connPool) {
    connectionPool = connPool;
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    String baseQuery = "select * from show_schemas_mock";

    QueryRunner run = new QueryRunner(true);
    List<SnowflakeSchema> schemas;
    try {
      if (namespace == null || namespace.isEmpty()) {
        final String finalQuery = baseQuery;
        schemas =
            connectionPool.run(
                conn -> run.query(conn, finalQuery, SnowflakeSchema.createHandler()));
      } else {
        final String finalQuery = baseQuery + " where database_name = ?";

        schemas =
            connectionPool.run(
                conn ->
                    run.query(
                        conn,
                        finalQuery,
                        SnowflakeSchema.createHandler(),
                        namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      }
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {

    String baseQuery = "select * from show_iceberg_tables_mock";

    QueryRunner run = new QueryRunner(true);

    List<SnowflakeTable> tables = Lists.newArrayList();
    try {
      if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
        String finalQuery = baseQuery + " where database_name = ? and schema_name = ?";
        tables.addAll(
            connectionPool.run(
                conn ->
                    run.query(
                        conn,
                        finalQuery,
                        SnowflakeTable.createHandler(),
                        namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                        namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1))));
      } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
        tables.addAll(
            connectionPool.run(conn -> run.query(conn, baseQuery, SnowflakeTable.createHandler())));
        tables.removeIf(
            table ->
                !table
                    .getDatabase()
                    .equals(namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      } else {
        tables.addAll(
            connectionPool.run(conn -> run.query(conn, baseQuery, SnowflakeTable.createHandler())));
      }

    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    QueryRunner run = new QueryRunner(true);

    SnowflakeTableMetadata tableMeta;
    try {
      tableMeta =
          connectionPool.run(
              conn ->
                  run.query(
                      conn,
                      "select * from get_iceberg_table_information_mock where table_name = ? and database_name = ? and schema_name = ?",
                      SnowflakeTableMetadata.createHandler(),
                      tableIdentifier.name(),
                      tableIdentifier.namespace().level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                      tableIdentifier
                          .namespace()
                          .level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1)));
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return tableMeta;
  }

  @Override
  public void close() {
    connectionPool.close();
  }
}
