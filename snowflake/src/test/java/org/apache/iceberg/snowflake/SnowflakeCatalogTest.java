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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SnowflakeCatalogTest {

  public SnowflakeCatalogTest() {}

  static final String TEST_CATALOG_NAME = "slushLog";
  static final String TEST_SETUP_SCRIPT = "setup_mock_snowflakedb.sql";

  static final String TEST_CUSTOM_FILE_IO = "org.apache.iceberg.snowflake.TestFileIO";
  static String uri;
  static SnowflakeCatalog catalog;
  static MockedStatic<SnowflakeResources> mockedResources;

  @ClassRule public static TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {

    File dbFile;
    try {
      dbFile = folder.newFile(UUID.randomUUID().toString().replace("-", "") + ".db");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<String, String> properties = Maps.newHashMap();

    uri = "jdbc:sqlite:" + dbFile.getAbsolutePath();

    properties.put(CatalogProperties.URI, uri);

    mockedResources = Mockito.mockStatic(SnowflakeResources.class);
    mockedResources
        .when(() -> SnowflakeResources.getDefaultFileIoImpl())
        .thenReturn((TEST_CUSTOM_FILE_IO));

    try {
      Connection con = DriverManager.getConnection(uri);
      Path path =
          Paths.get(
              SnowflakeCatalogTest.class.getClassLoader().getResource(TEST_SETUP_SCRIPT).toURI());
      importSQLScript(con, new FileInputStream(path.toFile()));

      catalog = new SnowflakeCatalog();
      catalog.setQueryFactory(new TestQueryFactory(DriverManager.getConnection(uri)));
      catalog.initialize(TEST_CATALOG_NAME, properties);
      con.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static void importSQLScript(Connection conn, InputStream in) throws SQLException {
    Scanner scan = new Scanner(in);
    scan.useDelimiter("(;(\r)?\n)|(--\n)");
    Statement st = null;
    try {
      st = conn.createStatement();
      while (scan.hasNext()) {
        String line = scan.next();
        if (line.startsWith("/*!") && line.endsWith("*/")) {
          int indx = line.indexOf(' ');
          line = line.substring(indx + 1, line.length() - " */".length());
        }

        if (line.trim().length() > 0) {
          st.execute(line);
        }
      }
    } catch (Exception ex) {
      throw ex;
    } finally {
      if (st != null) {
        st.close();
      }
    }
  }

  @Test
  public void testListNamespace() {
    List<Namespace> namespaces = catalog.listNamespaces();
    Assert.assertEquals(
        Lists.newArrayList(
            Namespace.of("DB_1", "SCHEMA_1"),
            Namespace.of("DB_2", "SCHEMA_2"),
            Namespace.of("DB_3", "SCHEMA_3"),
            Namespace.of("DB_3", "SCHEMA_4")),
        namespaces);
  }

  @Test
  public void testListNamespaceWithinDB() {
    String dbName = "DB_1";
    List<Namespace> namespaces = catalog.listNamespaces(Namespace.of(dbName));
    Assert.assertEquals(Lists.newArrayList(Namespace.of(dbName, "SCHEMA_1")), namespaces);
  }

  @Test
  public void testListNamespaceWithinSchema() {
    String dbName = "DB_3";
    String schemaName = "SCHEMA_4";
    List<Namespace> namespaces = catalog.listNamespaces(Namespace.of(dbName, schemaName));
    Assert.assertEquals(Lists.newArrayList(Namespace.of(dbName, schemaName)), namespaces);
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tables = catalog.listTables(Namespace.empty());
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"),
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_2"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_3"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_4"),
            TableIdentifier.of("DB_3", "SCHEMA_3", "TAB_5"),
            TableIdentifier.of("DB_3", "SCHEMA_4", "TAB_6")),
        tables);
  }

  @Test
  public void testListTablesWithinDB() {
    String dbName = "DB_1";
    List<TableIdentifier> tables = catalog.listTables(Namespace.of(dbName));
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"),
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_2")),
        tables);
  }

  @Test
  public void testListTablesWithinSchema() {
    String dbName = "DB_2";
    String schemaName = "SCHEMA_2";
    List<TableIdentifier> tables = catalog.listTables(Namespace.of(dbName, schemaName));
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_3"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_4")),
        tables);
  }

  @Test
  public void testLoadTable() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_1", "SCHEMA_1"), "TAB_1"));
    Assert.assertEquals(table.location(), "tab_1/");
  }
}
