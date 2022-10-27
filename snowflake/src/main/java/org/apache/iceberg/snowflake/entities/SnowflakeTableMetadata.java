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

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.util.JsonUtil;

public class SnowflakeTableMetadata {
  private String metadataLocation;
  private String status;

  private String jsonVal;

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public void setMetadataLocation(String location) {
    metadataLocation = location;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public static ResultSetHandler<SnowflakeTableMetadata> createHandler() {
    return new ResultSetHandler<SnowflakeTableMetadata>() {
      @Override
      public SnowflakeTableMetadata handle(ResultSet rs) throws SQLException {
        if (!rs.next()) {
          return null;
        }

        SnowflakeTableMetadata schema = new SnowflakeTableMetadata();
        schema.jsonVal = rs.getString("LOCATION");
        schema.metadataLocation = getMetadataLocationFromJson(schema.jsonVal);
        schema.status = getStatusFromJson(schema.jsonVal);
        return schema;
      }
    };
  }

  public static String getMetadataLocationFromJson(String json) {
    return JsonUtil.parse(json, SnowflakeTableMetadata::getMetadataLocationFromJson);
  }

  public static String getMetadataLocationFromJson(JsonNode json) {
    return JsonUtil.getString("metadataLocation", json);
  }

  public static String getStatusFromJson(String json) {
    return JsonUtil.parse(json, SnowflakeTableMetadata::getStatusFromJson);
  }

  public static String getStatusFromJson(JsonNode json) {
    return JsonUtil.getString("status", json);
  }
}
