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
import java.io.IOException;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.util.JsonUtil;

public class SnowflakeTableMetadata {
  private String metadataLocation;
  private String status;

  private String rawJsonVal;

  public SnowflakeTableMetadata(String metadataLocation, String status, String rawJsonVal) {
    this.metadataLocation = metadataLocation;
    this.status = status;
    this.rawJsonVal = rawJsonVal;
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public String getStatus() {
    return status;
  }

  /**
   * Factory method for parsing a JSON string containing expected Snowflake table metadata into a
   * SnowflakeTableMetadata object.
   */
  public static SnowflakeTableMetadata parseJson(String json) {
    JsonNode parsedVal;
    try {
      parsedVal = JsonUtil.mapper().readValue(json, JsonNode.class);
    } catch (IOException ioe) {
      throw new IllegalArgumentException(String.format("Malformed JSON: %s", json), ioe);
    }

    String metadataLocation = JsonUtil.getString("metadataLocation", parsedVal);
    String status = JsonUtil.getStringOrNull("status", parsedVal);
    return new SnowflakeTableMetadata(metadataLocation, status, json);
  }

  public static ResultSetHandler<SnowflakeTableMetadata> createHandler() {
    return rs -> {
      if (!rs.next()) {
        return null;
      }

      String rawJsonVal = rs.getString("LOCATION");
      return SnowflakeTableMetadata.parseJson(rawJsonVal);
    };
  }
}
