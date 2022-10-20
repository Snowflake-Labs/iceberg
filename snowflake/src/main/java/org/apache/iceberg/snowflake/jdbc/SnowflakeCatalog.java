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

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);
  private Object conf;
  private String catalogName = SnowflakeUtils.DEFAULT_CATALOG_NAME;
  private Map<String, String> catalogProperties = null;
  private JdbcClientPool connections;
  private FileIO fileIO;

  public SnowflakeCatalog() {}

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {

    List<TableIdentifier> tableList = Lists.newArrayList();
    try {

      String query = SnowflakeUtils.LIST_TABLES_QUERY;

      if (!(namespace == null || namespace.isEmpty())) {
        query += " in " + namespace.toString();
      }

      tableList.addAll(
          SnowflakeUtils.fetch(
              connections,
              row ->
                  TableIdentifier.of(
                      row.getString(SnowflakeUtils.LIST_TABLES_DB_NAME_COL),
                      row.getString(SnowflakeUtils.LIST_TABLES_SCHEMA_NAME_COL),
                      row.getString(SnowflakeUtils.LIST_TABLES_TABLE_NAME_COL)),
              query));
    } catch (UncheckedSQLException | UncheckedInterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }
    return tableList;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      connections.run(
          conn -> {
            return conn.prepareStatement(
                    String.format(SnowflakeUtils.DROP_TABLE_QUERY, identifier.toString()))
                .execute();
          });
    } catch (SQLException | InterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      connections.run(
          conn -> {
            return conn.prepareStatement(
                    String.format(
                        SnowflakeUtils.RENAME_TABLE_QUERY, from.toString(), to.toString()))
                .execute();
          });
    } catch (SQLException | InterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    catalogProperties = properties;
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");

    if (name != null) {
      this.catalogName = name;
    }

    try {
      LOG.debug("Connecting to JDBC database {}", properties.get(CatalogProperties.URI));

      connections = new JdbcClientPool(uri, properties);
      connections.run(
          conn -> {
            return conn.prepareStatement(SnowflakeUtils.ENABLE_ICEBERG_AT_SESSION_LEVEL);
          });

      String fileIOImpl = SnowflakeUtils.DEFAULT_FILE_IO_IMPL;

      if (null != catalogProperties.get(CatalogProperties.FILE_IO_IMPL)) {
        fileIOImpl = catalogProperties.get(CatalogProperties.FILE_IO_IMPL);
      }

      fileIO = CatalogUtil.loadFileIO(fileIOImpl, catalogProperties, conf);
    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
    }
  }

  @Override
  public void close() throws IOException {
    connections.close();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeUtils.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't supports more than 2 levels of namespace");
    try {
      String dbName =
          namespace.length() >= SnowflakeUtils.NAMESPACE_DB_LEVEL
              ? namespace.level(SnowflakeUtils.NAMESPACE_DB_LEVEL - 1)
              : null;
      String schemaName =
          namespace.length() >= SnowflakeUtils.NAMESPACE_SCHEMA_LEVEL
              ? namespace.level(SnowflakeUtils.NAMESPACE_SCHEMA_LEVEL - 1)
              : null;

      if (dbName != null) {
        connections.run(
            conn -> {
              return conn.prepareStatement(
                      String.format(SnowflakeUtils.CREATE_DATABASE_QUERY, dbName))
                  .execute();
            });
      }

      if (schemaName != null) {
        connections.run(
            conn -> {
              return conn.prepareStatement(
                      String.format(SnowflakeUtils.CREATE_SCHEMA_QUERY, dbName + "." + schemaName))
                  .execute();
            });
      }
    } catch (SQLException | InterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeUtils.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't supports more than 2 levels of namespace");
    List<Namespace> namespaceList = Lists.newArrayList();
    try {
      String query = SnowflakeUtils.LIST_NAMESPACE_QUERY;

      if (namespace == null || namespace.isEmpty()) {
        query += " " + SnowflakeUtils.LIST_NAMESPACE_ACCOUNT_SCOPE;
      } else {
        query +=
            " "
                + SnowflakeUtils.LIST_NAMESPACE_DB_SCOPE
                + " "
                + namespace.level(SnowflakeUtils.NAMESPACE_DB_LEVEL - 1);
      }

      namespaceList.addAll(
          SnowflakeUtils.fetch(
              connections,
              row ->
                  Namespace.of(
                      row.getString(SnowflakeUtils.LIST_NAMESPACE_DB_NAME_COL),
                      row.getString(SnowflakeUtils.LIST_NAMESPACE_SCHEMA_NAME_COL)),
              query));
    } catch (UncheckedSQLException | UncheckedInterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }

    if (namespace.length() == SnowflakeUtils.MAX_NAMESPACE_DEPTH) {
      if (namespaceList.contains(namespace)) {
        return Arrays.asList(namespace);
      } else {
        return Lists.newArrayList();
      }
    }

    return namespaceList;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    List<Namespace> allNamespaces = listNamespaces(namespace);

    if (allNamespaces.size() == 0) {
      throw new NoSuchNamespaceException("Could not find namespace %s", namespace);
    }

    Map<String, String> nameSpaceMetadata = Maps.newHashMap();
    nameSpaceMetadata.put("name", namespace.toString());
    return nameSpaceMetadata;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeUtils.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't supports more than 2 levels of namespace");
    try {
      String dbName =
          namespace.length() >= SnowflakeUtils.NAMESPACE_DB_LEVEL
              ? namespace.level(SnowflakeUtils.NAMESPACE_DB_LEVEL - 1)
              : null;
      String schemaName =
          namespace.length() >= SnowflakeUtils.NAMESPACE_SCHEMA_LEVEL
              ? namespace.level(SnowflakeUtils.NAMESPACE_SCHEMA_LEVEL - 1)
              : null;

      if (schemaName != null) {
        connections.run(
            conn -> {
              return conn.prepareStatement(
                      String.format(SnowflakeUtils.DROP_SCHEMA_QUERY, dbName + "." + schemaName))
                  .execute();
            });
      } else if (dbName != null) {
        connections.run(
            conn -> {
              return conn.prepareStatement(
                      String.format(SnowflakeUtils.DROP_DATABASE_QUERY, dbName))
                  .execute();
            });
      }
    } catch (SQLException | InterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
      return false;
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return false;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new SnowflakeTableOperations(
        connections, fileIO, catalogProperties, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  public Object getConf() {
    return conf;
  }
}
