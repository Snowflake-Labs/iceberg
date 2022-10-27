
DROP TABLE IF EXISTS show_schemas_mock;
DROP TABLE IF EXISTS show_iceberg_show_iceberg_tables_mock_mock;
DROP TABLE IF EXISTS get_iceberg_table_information_mock;


CREATE TABLE show_schemas_mock
  (
    created_on          DATETIME,
    name                VARCHAR(254) NOT NULL,
    is_default          CHAR(1) NOT NULL,
    is_current          CHAR(1) NOT NULL,
    database_name       VARCHAR(254) NOT NULL,
    owner               VARCHAR(254),
    comment             VARCHAR(1024),
    options             VARCHAR(20),
    retention_time      INT,
    owner_role_type     VARCHAR(254)
  );

INSERT INTO show_schemas_mock VALUES ('2022-08-07 05:55:27', 'SCHEMA_1', 'N' , 'Y', 'DB_1', 'ADMIN_1', '', NULL, 4, 'ROLE_1');
INSERT INTO show_schemas_mock VALUES ('2022-04-05 22:18:29', 'SCHEMA_2', 'N' , 'Y', 'DB_2', 'ADMIN_2', NULL, NULL, 1, 'ROLE_2');
INSERT INTO show_schemas_mock VALUES ('2022-05-02 22:12:05', 'SCHEMA_3', 'Y' , 'N', 'DB_3', 'ADMIN_4', '', '7', 1, 'ROLE_3');
INSERT INTO show_schemas_mock VALUES ('2021-11-26 12:50:45', 'SCHEMA_4', 'N' , 'N', 'DB_3', 'ADMIN_2', '', NULL, 1, 'ROLE_4');

CREATE TABLE show_iceberg_tables_mock
  (
    created_on            DATETIME,
    name                  VARCHAR(254) NOT NULL,
    database_name         VARCHAR(254) NOT NULL,
    schema_name           VARCHAR(254) NOT NULL,
    external_volume_name  VARCHAR(254) NOT NULL,
    owner                 VARCHAR(254),
    comment               VARCHAR(1024)
  );

  INSERT INTO show_iceberg_tables_mock VALUES ('2022-07-05 07:53:20', 'TAB_1', 'DB_1', 'SCHEMA_1', 'EXT_VOL1', 'ADMIN_1', 'iceberg vol');
  INSERT INTO show_iceberg_tables_mock VALUES ('2021-11-12 08:25:07', 'TAB_2', 'DB_1', 'SCHEMA_1', 'EXT_VOL1', 'ADMIN_2', NULL);
  INSERT INTO show_iceberg_tables_mock VALUES ('2022-03-23 05:38:57', 'TAB_3', 'DB_2', 'SCHEMA_2', 'EXT_VOL2', 'ADMIN_4', '');
  INSERT INTO show_iceberg_tables_mock VALUES ('2022-09-20 09:26:44', 'TAB_4', 'DB_2', 'SCHEMA_2', 'EXT_VOL2', 'ADMIN_2', 'table 4');
  INSERT INTO show_iceberg_tables_mock VALUES ('2022-04-03 03:27:51', 'TAB_5', 'DB_3', 'SCHEMA_3', 'EXT_VOL3', 'ADMIN_2', NULL);
  INSERT INTO show_iceberg_tables_mock VALUES ('2021-11-18 01:54:44', 'TAB_6', 'DB_3', 'SCHEMA_4', 'EXT_VOL3', 'ADMIN_2', '');

CREATE TABLE get_iceberg_table_information_mock
  (
    table_name         VARCHAR(254) NOT NULL,
    database_name         VARCHAR(254) NOT NULL,
    schema_name           VARCHAR(254) NOT NULL,
    location      VARCHAR(2048) NOT NULL
  );

  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_1', 'DB_1', 'SCHEMA_1', '{"metadataLocation":"Tab_1/metadata/v3.metadata.json","status":"success"}');
  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_2', 'DB_1', 'SCHEMA_1', '{"metadataLocation":"Tab_2/metadata/v1.metadata.json","status":"success"}');
  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_3', 'DB_2', 'SCHEMA_2', '{"metadataLocation":"Tab_3/metadata/v334.metadata.json","status":"success"}');
  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_4', 'DB_2', 'SCHEMA_2', '{"metadataLocation":"Tab_4/metadata/v323.metadata.json","status":"success"}');
  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_5', 'DB_3', 'SCHEMA_3', '{"metadataLocation":"Tab_5/metadata/v793.metadata.json","status":"success"}');
  INSERT INTO get_iceberg_table_information_mock VALUES ( 'TAB_6', 'DB_3', 'SCHEMA_4', '{"metadataLocation":"Tab_6/metadata/v123.metadata.json","status":"success"}');