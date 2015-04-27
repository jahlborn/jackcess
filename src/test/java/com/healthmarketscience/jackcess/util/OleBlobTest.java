/*
Copyright (c) 2013 James Ahlborn

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Arrays;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.CompoundOleUtil;
import junit.framework.TestCase;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import static com.healthmarketscience.jackcess.TestUtil.*;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;

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
          blob = row.getBlob("ole");
          OleBlob.Content content = blob.getContent();
          assertSame(blob, content.getBlob());
          assertSame(content, blob.getContent());

          switch(row.getInt("id")) {
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

  public void testReadBlob() throws Exception
  {
    for(TestDB testDb : TestDB.getSupportedForBasename(Basename.BLOB, true)) {
      Database db = open(testDb);

      Table t = db.getTable("Table1");

      for(Row row : t) {

        OleBlob oleBlob = null;
        try {

          String name = row.getString("name");
          oleBlob = row.getBlob("ole_data");
          OleBlob.Content content = oleBlob.getContent();
          Attachment attach = null;
          if(content.getType() != OleBlob.ContentType.LINK) {
            attach = row.getForeignKey("attach_data").getAttachments().get(0);
          }

          switch(content.getType()) {
          case LINK:
            OleBlob.LinkContent lc = (OleBlob.LinkContent)content;
            if("test_link".equals(name)) {
              assertEquals("Z:\\jackcess_test\\ole\\test_data.txt", lc.getLinkPath());
            } else {
              assertEquals("Z:\\jackcess_test\\ole\\test_datau2.txt", lc.getLinkPath());
            }
            break;

          case SIMPLE_PACKAGE:
            OleBlob.SimplePackageContent spc = (OleBlob.SimplePackageContent)content;
            byte[] packageBytes = toByteArray(spc.getStream(), spc.length());
            assertTrue(Arrays.equals(attach.getFileData(), packageBytes));
            break;

          case COMPOUND_STORAGE:
            OleBlob.CompoundContent cc = (OleBlob.CompoundContent)content;
            if(cc.hasContentsEntry()) {
              OleBlob.CompoundContent.Entry entry = cc.getContentsEntry();
              byte[] entryBytes = toByteArray(entry.getStream(), entry.length());
              assertTrue(Arrays.equals(attach.getFileData(), entryBytes));
            } else {

              if("test_word.doc".equals(name)) {
                checkCompoundEntries(cc, 
                                     "/%02OlePres000", 466,
                                     "/WordDocument", 4096,
                                     "/%05SummaryInformation", 4096,
                                     "/%05DocumentSummaryInformation", 4096,
                                     "/%03AccessObjSiteData", 56,
                                     "/%02OlePres001", 1620,
                                     "/1Table", 6380,
                                     "/%01CompObj", 114,
                                     "/%01Ole", 20);
                checkCompoundStorage(cc, attach);
              } else if("test_excel.xls".equals(name)) {
                checkCompoundEntries(cc, 
                                     "/%02OlePres000", 1326,
                                     "/%03AccessObjSiteData", 56,
                                     "/%05SummaryInformation", 200,
                                     "/%05DocumentSummaryInformation", 264,
                                     "/%02OlePres001", 4208,
                                     "/%01CompObj", 107,
                                     "/Workbook", 13040,
                                     "/%01Ole", 20);
                // the excel data seems to be modified when embedded as ole,
                // so we can't reallly test it against the attachment data
              } else {
                throw new RuntimeException("unexpected compound entry " + name);
              }
            }
            break;

          case OTHER:
            OleBlob.OtherContent oc = (OleBlob.OtherContent)content;
            byte[] otherBytes = toByteArray(oc.getStream(), oc.length());
            assertTrue(Arrays.equals(attach.getFileData(), otherBytes));
            break;

          default:
            throw new RuntimeException("unexpected type " + content.getType());
          }

        } finally {
          ByteUtil.closeQuietly(oleBlob);
        }
      }

      db.close();
    } 
  }

  private static void checkCompoundEntries(OleBlob.CompoundContent cc, 
                                           Object... entryInfo)
    throws Exception
  {
    int idx = 0;
    for(OleBlob.CompoundContent.Entry e : cc) {
      String entryName = (String)entryInfo[idx];
      int entryLen = (Integer)entryInfo[idx + 1];

      assertEquals(entryName, e.getName());
      assertEquals(entryLen, e.length());

      idx += 2;
    }
  }

  private static void checkCompoundStorage(OleBlob.CompoundContent cc, 
                                           Attachment attach)
    throws Exception
  {
    File tmpData = File.createTempFile("attach_", ".dat");

    try {
      FileOutputStream fout = new FileOutputStream(tmpData);
      fout.write(attach.getFileData());
      fout.close();

      NPOIFSFileSystem attachFs = new NPOIFSFileSystem(tmpData, true);

      for(OleBlob.CompoundContent.Entry e : cc) {
        DocumentEntry attachE = null;
        try {
          attachE = CompoundOleUtil.getDocumentEntry(e.getName(), attachFs.getRoot());
        } catch(FileNotFoundException fnfe) {
          // ignored, the ole data has extra entries
          continue;
        }

        byte[] attachEBytes = toByteArray(new DocumentInputStream(attachE), 
                                          attachE.getSize());
        byte[] entryBytes = toByteArray(e.getStream(), e.length());

        assertTrue(Arrays.equals(attachEBytes, entryBytes));
      }

      ByteUtil.closeQuietly(attachFs);
      
    } finally {
      tmpData.delete();
    }    
  }
}
