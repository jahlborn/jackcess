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

import static com.healthmarketscience.jackcess.DatabaseTest.*;

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
    String sampleFilePath = sampleFileStr.getAbsolutePath();
    String sampleFileName = sampleFile.getName();
    byte[] sampleFileBytes = toByteArray(sampleFile);

    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
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
        OleBlob blob = null;
        try {
          blob = OleBlob.Builder.fromInternalData(
              (byte[])row.get("ole"));
          Content content = blob.getContent();
          assertSame(blob, content.getBlob());
          assertSame(content, blob.getContent());

          switch((Integer)row.get("id")) {
          case 1:
            assertEquals(OleBlob.ContentType.SIMPLE_PACKAGE, content.getType());
            assertEquals(sampleFilePath, content.getFilePath());
            assertEquals(sampleFilePath, content.getLocalFilePath());
            assertEquals(sampleFileName, content.getFileName());
            assertEquals(OleBlob.Builder.PACKAGE_PRETTY_NAME, 
                         content.getPrettyName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         content.getTypeName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         content.getClassName());
            assertEquals(sampleFileBytes.length, content.length());
            assertEquals(sampleFileBytes, toByteArray(content.getStream()));
            break;
          case 2:
            assertEquals(OleBlob.ContentType.LINK, content.getType());
            assertEquals(sampleFilePath, content.getLinkPath());
            assertEquals(sampleFilePath, content.getFilePath());
            assertEquals(sampleFileName, content.getFileName());
            assertEquals(OleBlob.Builder.PACKAGE_PRETTY_NAME, 
                         content.getPrettyName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         content.getTypeName());
            assertEquals(OleBlob.Builder.PACKAGE_TYPE_NAME, 
                         content.getClassName());
            break;
          case 3:
            assertEquals(OleBlob.ContentType.OTHER, content.getType());
            assertEquals("Text File", content.getPrettyName());
            assertEquals("Text.File", content.getClassName());
            assertEquals("TextFile", content.getTypeName());
            assertEquals(sampleFileBytes.length, content.length());
            assertEquals(sampleFileBytes, toByteArray(content.getStream()));
            break;
          default:
            throw new RuntimeException("unexpected id " + row);
          }
        } finally {
          ByteUtil.closeQuietly(oleBlob);
        }
      }

      db.close();      
    }    
  }
}
