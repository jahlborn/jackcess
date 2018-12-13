/*
Copyright (c) 2018 James Ahlborn

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

package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.RowImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.util.LinkResolver;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static com.healthmarketscience.jackcess.Database.*;

/**
 *
 * @author James Ahlborn
 */
public class LocalDateTimeTest extends TestCase
{
  public LocalDateTimeTest(String name) throws Exception {
    super(name);
  }

  public void testAncientDates() throws Exception
  {
    ZoneId zoneId = ZoneId.of("America/New_York");
    DateTimeFormatter sdf = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    List<String> dates = Arrays.asList("1582-10-15", "1582-10-14",
                                       "1492-01-10", "1392-01-10");

    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      db.setZoneId(zoneId);
      db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      for(String dateStr : dates) {
        LocalDate ld = LocalDate.parse(dateStr, sdf);
        table.addRow("row " + dateStr, ld);
      }

      List<String> foundDates = new ArrayList<String>();
      for(Row row : table) {
        foundDates.add(sdf.format(row.getLocalDateTime("date")));
      }

      assertEquals(dates, foundDates);

      db.close();
    }

    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.OLD_DATES)) {
      Database db = openCopy(testDB);
      db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

      Table t = db.getTable("Table1");

      List<String> foundDates = new ArrayList<String>();
      for(Row row : t) {
        foundDates.add(sdf.format(row.getLocalDateTime("DateField")));
      }

      assertEquals(dates, foundDates);

      db.close();
    }

  }

  public void testZoneId() throws Exception
  {
    ZoneId zoneId = ZoneId.of("America/New_York");
    doTestZoneId(zoneId);

    zoneId = ZoneId.of("Australia/Sydney");
    doTestZoneId(zoneId);
  }

  private static void doTestZoneId(final ZoneId zoneId) throws Exception
  {
    final TimeZone tz = TimeZone.getTimeZone(zoneId);
    ColumnImpl col = new ColumnImpl(null, null, DataType.SHORT_DATE_TIME, 0, 0, 0) {
      @Override
      protected TimeZone getTimeZone() { return tz; }
      @Override
      protected ZoneId getZoneId() { return zoneId; }
    };

    SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
    df.setTimeZone(tz);

    long startDate = df.parse("2012.01.01").getTime();
    long endDate = df.parse("2013.01.01").getTime();

    Calendar curCal = Calendar.getInstance(tz);
    curCal.setTimeInMillis(startDate);

    DateTimeFormatter sdf = DateTimeFormatter.ofPattern("uuuu.MM.dd HH:mm:ss");

    while(curCal.getTimeInMillis() < endDate) {
      Date curDate = curCal.getTime();
      LocalDateTime curLdt = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(curDate.getTime()), zoneId);
      LocalDateTime newLdt = ColumnImpl.ldtFromLocalDateDouble(
          col.toDateDouble(curDate));
      if(!curLdt.equals(newLdt)) {
        System.out.println("FOO " + curLdt + " " + newLdt);
        assertEquals(sdf.format(curLdt), sdf.format(newLdt));
      }
      curCal.add(Calendar.MINUTE, 30);
    }
  }

}
