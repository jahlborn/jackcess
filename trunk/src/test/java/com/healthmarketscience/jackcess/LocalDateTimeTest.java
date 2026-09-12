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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import junit.framework.TestCase;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import static com.healthmarketscience.jackcess.Database.*;
import static com.healthmarketscience.jackcess.DatabaseBuilder.*;

/**
 *
 * @author James Ahlborn
 */
public class LocalDateTimeTest extends TestCase
{
  public LocalDateTimeTest(String name) throws Exception {
    super(name);
  }

  public void testWriteAndReadLocalDate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

      Table table = newTable("test")
        .addColumn(newColumn("name", DataType.TEXT))
        .addColumn(newColumn("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      // since jackcess does not really store millis, shave them off before
      // storing the current date/time
      long curTimeNoMillis = (System.currentTimeMillis() / 1000L);
      curTimeNoMillis *= 1000L;

      DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
      List<Date> dates =
        new ArrayList<Date>(
            Arrays.asList(
                df.parse("19801231 00:00:00"),
                df.parse("19930513 14:43:27"),
                null,
                df.parse("20210102 02:37:00"),
                new Date(curTimeNoMillis)));

      Calendar c = Calendar.getInstance();
      for(int year = 1801; year < 2050; year +=3) {
        for(int month = 0; month <= 12; ++month) {
          for(int day = 1; day < 29; day += 3) {
            c.clear();
            c.set(Calendar.YEAR, year);
            c.set(Calendar.MONTH, month);
            c.set(Calendar.DAY_OF_MONTH, day);
            dates.add(c.getTime());
          }
        }
      }

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(Date d : dates) {
          table.addRow("row " + d, d);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      List<LocalDateTime> foundDates = new ArrayList<LocalDateTime>();
      for(Row row : table) {
        foundDates.add(row.getLocalDateTime("date"));
      }

      assertEquals(dates.size(), foundDates.size());
      for(int i = 0; i < dates.size(); ++i) {
        Date expected = dates.get(i);
        LocalDateTime found = foundDates.get(i);
        assertSameDate(expected, found);
      }

      db.close();
    }
  }

  public void testAncientLocalDates() throws Exception
  {
    ZoneId zoneId = ZoneId.of("America/New_York");
    DateTimeFormatter sdf = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    List<String> dates = Arrays.asList("1582-10-15", "1582-10-14",
                                       "1492-01-10", "1392-01-10");

    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      db.setZoneId(zoneId);
      db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

      Table table = newTable("test")
        .addColumn(newColumn("name", DataType.TEXT))
        .addColumn(newColumn("date", DataType.SHORT_DATE_TIME))
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
      public TimeZone getTimeZone() { return tz; }
      @Override
      public ZoneId getZoneId() { return zoneId; }
      @Override
      public ColumnImpl.DateTimeFactory getDateTimeFactory() {
        return getDateTimeFactory(DateTimeType.LOCAL_DATE_TIME);
      }
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

  public void testWriteAndReadTemporals() throws Exception {
    ZoneId zoneId = ZoneId.of("America/New_York");
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      db.setZoneId(zoneId);
      db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);

      Table table = newTable("test")
        .addColumn(newColumn("name", DataType.TEXT))
        .addColumn(newColumn("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      // since jackcess does not really store millis, shave them off before
      // storing the current date/time
      long curTimeNoMillis = (System.currentTimeMillis() / 1000L);
      curTimeNoMillis *= 1000L;

      DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
      List<Date> tmpDates =
        new ArrayList<Date>(
            Arrays.asList(
                df.parse("19801231 00:00:00"),
                df.parse("19930513 14:43:27"),
                df.parse("20210102 02:37:00"),
                new Date(curTimeNoMillis)));

      List<Object> objs = new ArrayList<Object>();
      List<LocalDateTime> expected = new ArrayList<LocalDateTime>();
      for(Date d : tmpDates) {
        Instant inst = Instant.ofEpochMilli(d.getTime());
        objs.add(inst);
        ZonedDateTime zdt = inst.atZone(zoneId);
        objs.add(zdt);
        LocalDateTime ldt = zdt.toLocalDateTime();
        objs.add(ldt);

        for(int i = 0; i < 3; ++i) {
          expected.add(ldt);
        }
      }

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(Object o : objs) {
          table.addRow("row " + o, o);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      List<LocalDateTime> foundDates = new ArrayList<LocalDateTime>();
      for(Row row : table) {
        foundDates.add(row.getLocalDateTime("date"));
      }

      assertEquals(expected, foundDates);

      db.close();
    }
  }

}
