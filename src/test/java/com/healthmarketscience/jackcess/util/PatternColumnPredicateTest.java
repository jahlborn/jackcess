/*
Copyright (c) 2020 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class PatternColumnPredicateTest extends TestCase
{

  public PatternColumnPredicateTest(String name) {
    super(name);
  }

  public void testRegexPredicate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createTestDb(fileFormat);

      Table t = db.getTable("Test");

      assertEquals(
          Arrays.asList("Foo", "some row", "aNoThEr row", "nonsense"),
          findRowsByPattern(
              t, PatternColumnPredicate.forJavaRegex(".*o.*")));

      assertEquals(
          Arrays.asList("Bar", "0102", "FOO", "BAR", "67", "bunch_13_data", "42 is the ANSWER", "[try] matching t.h+i}s"),
          findRowsByPattern(
              t, PatternColumnPredicate.forJavaRegex(".*o.*").negate()));

      assertEquals(
          Arrays.asList("Foo", "some row", "FOO", "aNoThEr row", "nonsense"),
          findRowsByPattern(
              t, PatternColumnPredicate.forAccessLike("*o*")));

      assertEquals(
          Arrays.asList("0102", "67", "bunch_13_data", "42 is the ANSWER"),
          findRowsByPattern(
              t, PatternColumnPredicate.forAccessLike("*##*")));

      assertEquals(
          Arrays.asList("42 is the ANSWER"),
          findRowsByPattern(
              t, PatternColumnPredicate.forAccessLike("## *")));

      assertEquals(
          Arrays.asList("Foo"),
          findRowsByPattern(
              t, PatternColumnPredicate.forSqlLike("F_o")));

      assertEquals(
          Arrays.asList("Foo", "FOO"),
          findRowsByPattern(
              t, PatternColumnPredicate.forSqlLike("F_o", true)));

      assertEquals(
          Arrays.asList("[try] matching t.h+i}s"),
          findRowsByPattern(
              t, PatternColumnPredicate.forSqlLike("[try] % t.h+i}s")));

      assertEquals(
          Arrays.asList("bunch_13_data"),
          findRowsByPattern(
              t, PatternColumnPredicate.forSqlLike("bunch\\_%\\_data")));

      db.close();
    }
  }

  private static List<String> findRowsByPattern(
      Table t, Predicate<Object> pred) {
    return t.getDefaultCursor().newIterable()
      .setMatchPattern("data", pred)
      .stream()
      .map(r -> r.getString("data"))
      .collect(Collectors.toList());
  }

  private static Database createTestDb(FileFormat fileFormat) throws Exception {
    Database db = create(fileFormat);

    Table table = new TableBuilder("Test")
      .addColumn(new ColumnBuilder("id", DataType.LONG).setAutoNumber(true))
      .addColumn(new ColumnBuilder("data", DataType.TEXT))
      .setPrimaryKey("id")
      .toTable(db);

    table.addRow(Column.AUTO_NUMBER, "Foo");
    table.addRow(Column.AUTO_NUMBER, "some row");
    table.addRow(Column.AUTO_NUMBER, "Bar");
    table.addRow(Column.AUTO_NUMBER, "0102");
    table.addRow(Column.AUTO_NUMBER, "FOO");
    table.addRow(Column.AUTO_NUMBER, "BAR");
    table.addRow(Column.AUTO_NUMBER, "67");
    table.addRow(Column.AUTO_NUMBER, "aNoThEr row");
    table.addRow(Column.AUTO_NUMBER, "bunch_13_data");
    table.addRow(Column.AUTO_NUMBER, "42 is the ANSWER");
    table.addRow(Column.AUTO_NUMBER, "[try] matching t.h+i}s");
    table.addRow(Column.AUTO_NUMBER, "nonsense");

    return db;
  }
}
