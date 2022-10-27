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
package org.apache.iceberg.snowflake.entities;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnowflakeTable {
  private String name;
  private String databaseName;
  private String schemaName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDatabase() {
    return databaseName;
  }

  public void setDatabaseName(String dbName) {
    databaseName = dbName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public static ResultSetHandler<List<SnowflakeTable>> createHandler() {
    return new ResultSetHandler<List<SnowflakeTable>>() {
      @Override
      public List<SnowflakeTable> handle(ResultSet rs) throws SQLException {

        List<SnowflakeTable> schemas = Lists.newArrayList();
        while (rs.next()) {
          SnowflakeTable schema = new SnowflakeTable();
          schema.name = rs.getString("name");
          schema.databaseName = rs.getString("database_name");
          schema.schemaName = rs.getString("schema_name");
          schemas.add(schema);
        }
        return schemas;
      }
    };
  }
}
