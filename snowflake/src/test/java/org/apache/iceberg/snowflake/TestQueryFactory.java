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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;

public class TestQueryFactory implements QueryFactory {
  private Connection connection;

  TestQueryFactory(Connection connections) {
    connection = connections;
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    String query = "select * from show_schemas_mock";

    QueryRunner run = new QueryRunner(true);
    List<SnowflakeSchema> schemas = null;
    try {
      if (namespace == null || namespace.isEmpty()) {
        schemas = run.query(connection, query, SnowflakeSchema.createHandler());
      } else {
        query += " where database_name = ?";

        schemas =
            run.query(
                connection,
                query,
                SnowflakeSchema.createHandler(),
                namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1));
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {

    String query = "select * from show_iceberg_tables_mock";

    QueryRunner run = new QueryRunner(true);

    List<SnowflakeTable> tables = Lists.newArrayList();
    try {
      if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
        query += " where database_name = ? and schema_name = ?";
        tables.addAll(
            run.query(
                connection,
                query,
                SnowflakeTable.createHandler(),
                namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1)));
      } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
        tables.addAll(run.query(connection, query, SnowflakeTable.createHandler()));
        tables.removeIf(
            table ->
                !table
                    .getDatabase()
                    .equals(namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)));
      } else {
        tables.addAll(run.query(connection, query, SnowflakeTable.createHandler()));
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    QueryRunner run = new QueryRunner(true);

    SnowflakeTableMetadata tableMeta = null;
    try {
      tableMeta =
          run.query(
              connection,
              "select * from get_iceberg_table_information_mock where table_name = ? and database_name = ? and schema_name = ?",
              tableMeta.createHandler(),
              tableIdentifier.name(),
              tableIdentifier.namespace().level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
              tableIdentifier.namespace().level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return tableMeta;
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
