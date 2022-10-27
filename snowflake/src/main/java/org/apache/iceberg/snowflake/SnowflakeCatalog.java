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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);
  private Object conf;
  private String catalogName = SnowflakeResources.DEFAULT_CATALOG_NAME;
  private Map<String, String> catalogProperties = null;
  private FileIO fileIO;
  private QueryFactory queryFactory;

  private JdbcClientPool connectionPool;

  public SnowflakeCatalog() {}

  public void setQueryFactory(QueryFactory factory) {
    queryFactory = factory;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {

    List<SnowflakeTable> sfTables = queryFactory.listIcebergTables(namespace);

    return sfTables.stream()
        .map(
            table ->
                TableIdentifier.of(table.getDatabase(), table.getSchemaName(), table.getName()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    catalogProperties = properties;

    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");

    if (name != null) {
      this.catalogName = name;
    }

    LOG.debug("Connecting to JDBC database {}", properties.get(CatalogProperties.URI));

    connectionPool = new JdbcClientPool(uri, properties);

    if (queryFactory == null) {
      queryFactory = new SnowflakeQueryFactory(connectionPool);
    }

    String fileIOImpl = SnowflakeResources.getDefaultFileIoImpl();

    if (null != catalogProperties.get(CatalogProperties.FILE_IO_IMPL)) {
      fileIOImpl = catalogProperties.get(CatalogProperties.FILE_IO_IMPL);
    }

    fileIO = CatalogUtil.loadFileIO(fileIOImpl, catalogProperties, conf);
  }

  @Override
  public void close() throws IOException {
    queryFactory.close();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {}

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't supports more than 2 levels of namespace");
    List<Namespace> namespaceList = Lists.newArrayList();
    try {
      List<SnowflakeSchema> sfSchemas = queryFactory.listSchemas(namespace);
      namespaceList =
          sfSchemas.stream()
              .map(schema -> Namespace.of(schema.getDatabase(), schema.getName()))
              .collect(Collectors.toList());
    } catch (UncheckedSQLException | UncheckedInterruptedException ex) {
      LOG.error("{}", ex.toString(), ex);
    }

    if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
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
    return false;
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
        queryFactory, fileIO, catalogProperties, catalogName, tableIdentifier);
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
