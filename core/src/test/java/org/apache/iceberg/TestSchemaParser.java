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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSchemaParser extends DataTest {

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected boolean supportsGeospatial() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  public void testSchemaId() {
    Schema schema = new Schema(34, required(1, "id", Types.LongType.get()));

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.schemaId()).isEqualTo(schema.schemaId());
  }

  @Test
  public void testIdentifierColumns() {
    Schema schema =
        new Schema(
            Lists.newArrayList(
                required(1, "id-1", Types.LongType.get()),
                required(2, "id-2", Types.LongType.get()),
                optional(3, "data", Types.StringType.get())),
            Sets.newHashSet(1, 2));

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.identifierFieldIds()).isEqualTo(Sets.newHashSet(1, 2));
  }

  @Test
  public void testDocStrings() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get(), "unique identifier"),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withDoc("payload")
                .build());

    Schema serialized = SchemaParser.fromJson(SchemaParser.toJson(schema));
    assertThat(serialized.findField("id").doc()).isEqualTo("unique identifier");
    assertThat(serialized.findField("data").doc()).isEqualTo("payload");
  }
}
