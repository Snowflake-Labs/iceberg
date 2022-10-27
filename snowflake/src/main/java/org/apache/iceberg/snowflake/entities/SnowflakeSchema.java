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

public class SnowflakeSchema {
  private String name;
  private String databaseName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDatabase() {
    return databaseName;
  }

  public void setDatabase(String dbname) {
    this.databaseName = dbname;
  }

  public static ResultSetHandler<List<SnowflakeSchema>> createHandler() {
    return new ResultSetHandler<List<SnowflakeSchema>>() {
      @Override
      public List<SnowflakeSchema> handle(ResultSet rs) throws SQLException {

        List<SnowflakeSchema> schemas = Lists.newArrayList();
        while (rs.next()) {
          SnowflakeSchema schema = new SnowflakeSchema();
          schema.name = rs.getString("name");
          schema.databaseName = rs.getString("database_name");
          schemas.add(schema);
        }
        return schemas;
      }
    };
  }
}
