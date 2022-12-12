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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class SnowflakeTableMetadata {
  public static final Pattern SNOWFLAKE_AZURE_PATTERN =
      Pattern.compile("azure://([^/]+)/([^/]+)/(.*)");

  private String snowflakeMetadataLocation;
  private String status;
  private String icebergMetadataLocation;

  private String rawJsonVal;

  public SnowflakeTableMetadata(
      String snowflakeMetadataLocation,
      String icebergMetadataLocation,
      String status,
      String rawJsonVal) {
    this.snowflakeMetadataLocation = snowflakeMetadataLocation;
    this.icebergMetadataLocation = icebergMetadataLocation;
    this.status = status;
    this.rawJsonVal = rawJsonVal;
  }

  /** Storage location of table metadata in Snowflake's path syntax. */
  public String getSnowflakeMetadataLocation() {
    return snowflakeMetadataLocation;
  }

  /** Storage location of table metadata in Iceberg's path syntax. */
  public String getIcebergMetadataLocation() {
    return icebergMetadataLocation;
  }

  public String getStatus() {
    return status;
  }

  /**
   * Translates from Snowflake's path syntax to Iceberg's path syntax for paths matching known
   * non-compatible Snowflake paths. Throws IllegalArgumentException if the prefix of the
   * snowflakeLocation is a known non-compatible path syntax but fails to match the expected path
   * components for a successful translation.
   */
  public static String getIcebergLocationFromSnowflakeLocation(String snowflakeLocation) {
    if (snowflakeLocation.startsWith("azure://")) {
      // Convert from expected path of the form:
      // azure://account.blob.core.windows.net/container/volumepath
      // to:
      // wasbs://container@account.blob.core.windows.net/volumepath
      Matcher matcher = SNOWFLAKE_AZURE_PATTERN.matcher(snowflakeLocation);
      Preconditions.checkArgument(
          matcher.matches(),
          "Location '%s' failed to match pattern '%s'",
          snowflakeLocation,
          SNOWFLAKE_AZURE_PATTERN);
      return String.format(
          "wasbs://%s@%s/%s", matcher.group(2), matcher.group(1), matcher.group(3));
    } else if (snowflakeLocation.startsWith("gcs://")) {
      // Convert from expected path of the form:
      // gcs://bucket/path
      // to:
      // gs://bucket/path
      return "gs" + snowflakeLocation.substring(3);
    }
    return snowflakeLocation;
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

    String snowflakeMetadataLocation = JsonUtil.getString("metadataLocation", parsedVal);
    String status = JsonUtil.getStringOrNull("status", parsedVal);

    String icebergMetadataLocation =
        getIcebergLocationFromSnowflakeLocation(snowflakeMetadataLocation);

    return new SnowflakeTableMetadata(
        snowflakeMetadataLocation, icebergMetadataLocation, status, json);
  }

  public static ResultSetHandler<SnowflakeTableMetadata> createHandler() {
    return rs -> {
      if (!rs.next()) {
        return null;
      }

      String rawJsonVal = rs.getString("METADATA");
      return SnowflakeTableMetadata.parseJson(rawJsonVal);
    };
  }
}
