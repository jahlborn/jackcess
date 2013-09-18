/*
Copyright (c) 2013 James Ahlborn

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA
*/

package com.healthmarketscience.jackcess.util;

import java.io.File;
import java.util.Arrays;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import static com.healthmarketscience.jackcess.DatabaseTest.*;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import junit.framework.TestCase;

/**
 *
 * @author James Ahlborn
 */
public class OleBlobTest extends TestCase 
{

  public OleBlobTest(String name) {
    super(name);
  }

  public void testCreateBlob() throws Exception
  {
    File sampleFile = new File("src/test/data/sample-input.tab");
    String sampleFilePath = sampleFile.getAbsolutePath();
    String sampleFileName = sampleFile.getName();
    byte[] sampleFileBytes = toByteArray(sampleFile);

    for(FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("TestOle")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("ole", DataType.OLE))
        .toTable(db);

      OleBlob blob = null;
      try {
        blob = new OleBlob.Builder()
          .setSimplePackage(sampleFile)
          .toBlob();
        t.addRow(1, blob);
      } finally {
        ByteUtil.closeQuietly(blob);
      }
      
      try {
        blob = new OleBlob.Builder()
          .setLink(sampleFile)
          .toBlob();
        t.addRow(2, blob);
      } finally {
        ByteUtil.closeQuietly(blob);
      }
      
      try {
        blob = new OleBlob.Builder()
          .setPackagePrettyName("Text File")
          .setPackageClassName("Text.File")
          .setPackageTypeName("TextFile")
          .setOtherBytes(sampleFileBytes)
          .toBlob();
        t.addRow(3, blob);
      } finally {
        ByteUtil.closeQuietly(blob);
      }

      for(Row row : t) {
        try {
          blob = OleBlob.Builder.fromInternalData(
              (byte[])row.get("ole"));
          OleBlob.Content content = blob.getContent();
          assertSame(blob, content.getBlob());
          assertSame(content, blob.getContent());

          switch((Integer)row.get("id")) {
          case 1:
            assertEquals(OleBlob.ContentType.SIMPLE_PACKAGE, content.getType());
            OleBlob.SimplePackageContent spc = (OleBlob.SimplePackageContent)content;
            assertEquals(sampleFilePath, spc.getFilePath());
            assertEquals(sampleFilePath, spc.getLocalFilePath());
            assertEquals(sampleFileName, spc.getFileName());
            assertEquals(OleBlob.Builder.PACKAGE_PRETTY_NAME, 
                         spc.getPrettyName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         spc.getTypeName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         spc.getClassName());
            assertEquals(sampleFileBytes.length, spc.length());
            assertTrue(Arrays.equals(sampleFileBytes, 
                                     toByteArray(spc.getStream(), spc.length())));
            break;

          case 2:
            OleBlob.LinkContent lc = (OleBlob.LinkContent)content;
            assertEquals(OleBlob.ContentType.LINK, lc.getType());
            assertEquals(sampleFilePath, lc.getLinkPath());
            assertEquals(sampleFilePath, lc.getFilePath());
            assertEquals(sampleFileName, lc.getFileName());
            assertEquals(OleBlob.Builder.PACKAGE_PRETTY_NAME, lc.getPrettyName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, lc.getTypeName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, lc.getClassName());
            break;
            
          case 3:
            OleBlob.OtherContent oc = (OleBlob.OtherContent)content;
            assertEquals(OleBlob.ContentType.OTHER, oc.getType());
            assertEquals("Text File", oc.getPrettyName());
            assertEquals("Text.File", oc.getClassName());
            assertEquals("TextFile", oc.getTypeName());
            assertEquals(sampleFileBytes.length, oc.length());
            assertTrue(Arrays.equals(sampleFileBytes, 
                                     toByteArray(oc.getStream(), oc.length())));
            break;
          default:
            throw new RuntimeException("unexpected id " + row);
          }
        } finally {
          ByteUtil.closeQuietly(blob);
        }
      }

      db.close();      
    }    
  }
}
