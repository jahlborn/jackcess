/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import com.healthmarketscience.jackcess.ByteUtil;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.PageChannel;
import com.healthmarketscience.jackcess.Table;


/**
 * Complex column info for a column holding 0 or more attachments per row.
 *
 * @author James Ahlborn
 */
public class AttachmentColumnInfo extends ComplexColumnInfo<Attachment>
{
  private static final String FILE_NAME_COL_NAME = "FileName";
  private static final String FILE_TYPE_COL_NAME = "FileType";

  private static final int DATA_TYPE_RAW = 0;
  private static final int DATA_TYPE_COMPRESSED = 1;

  private final Column _fileUrlCol;
  private final Column _fileNameCol;
  private final Column _fileTypeCol;
  private final Column _fileDataCol;
  private final Column _fileTimeStampCol;
  private final Column _fileFlagsCol;
  
  public AttachmentColumnInfo(Column column, int complexId,
                              Table typeObjTable, Table flatTable)
    throws IOException
  {
    super(column, complexId, typeObjTable, flatTable);

    Column fileUrlCol = null;
    Column fileNameCol = null;
    Column fileTypeCol = null;
    Column fileDataCol = null;
    Column fileTimeStampCol = null;
    Column fileFlagsCol = null;

    for(Column col : getTypeColumns()) {
      switch(col.getType()) {
      case TEXT:
        if(FILE_NAME_COL_NAME.equalsIgnoreCase(col.getName())) {
          fileNameCol = col;
        } else if(FILE_TYPE_COL_NAME.equalsIgnoreCase(col.getName())) {
          fileTypeCol = col;
        } else {
          // if names don't match, assign in order: name, type
          if(fileNameCol == null) {
            fileNameCol = col;
          } else if(fileTypeCol == null) {
            fileTypeCol = col;
          }
        }
        break;
      case LONG:
        fileFlagsCol = col;
        break;
      case SHORT_DATE_TIME:
        fileTimeStampCol = col;
        break;
      case OLE:
        fileDataCol = col;
        break;
      case MEMO:
        fileUrlCol = col;
        break;
      default:
        // ignore
      }
    }
    
    _fileUrlCol = fileUrlCol;
    _fileNameCol = fileNameCol;
    _fileTypeCol = fileTypeCol;
    _fileDataCol = fileDataCol;
    _fileTimeStampCol = fileTimeStampCol;
    _fileFlagsCol = fileFlagsCol;
  }

  public Column getFileUrlColumn() {
    return _fileUrlCol;
  }
  
  public Column getFileNameColumn() {
    return _fileNameCol;
  }

  public Column getFileTypeColumn() {
    return _fileTypeCol;
  }
  
  public Column getFileDataColumn() {
    return _fileDataCol;
  }
  
  public Column getFileTimeStampColumn() {
    return _fileTimeStampCol;
  }
  
  public Column getFileFlagsColumn() {
    return _fileFlagsCol;
  }  
  
  @Override
  public ComplexDataType getType()
  {
    return ComplexDataType.ATTACHMENT;
  }

  @Override
  protected AttachmentImpl toValue(ComplexValueForeignKey complexValueFk,
                                   Map<String,Object> rawValue) {
    int id = (Integer)getPrimaryKeyColumn().getRowValue(rawValue);
    String url = (String)getFileUrlColumn().getRowValue(rawValue);
    String name = (String)getFileNameColumn().getRowValue(rawValue);
    String type = (String)getFileTypeColumn().getRowValue(rawValue);
    Integer flags = (Integer)getFileFlagsColumn().getRowValue(rawValue);
    Date ts = (Date)getFileTimeStampColumn().getRowValue(rawValue);
    byte[] data = (byte[])getFileDataColumn().getRowValue(rawValue);
    
    return new AttachmentImpl(id, complexValueFk, url, name, type, data,
                              ts, flags, null);
  }

  @Override
  protected Object[] asRow(Object[] row, Attachment attachment) {
    super.asRow(row, attachment);
    getFileUrlColumn().setRowValue(row, attachment.getFileUrl());
    getFileNameColumn().setRowValue(row, attachment.getFileName());
    getFileTypeColumn().setRowValue(row, attachment.getFileType());
    getFileFlagsColumn().setRowValue(row, attachment.getFileFlags());
    getFileTimeStampColumn().setRowValue(row, attachment.getFileTimeStamp());
    getFileDataColumn().setRowValue(row, attachment.getFileData());
    return row;
  }

  public static Attachment newAttachment(byte[] data) {
    return newAttachment(INVALID_COMPLEX_VALUE_ID, data);
  }
  
  public static Attachment newAttachment(ComplexValueForeignKey complexValueFk,
                                         byte[] data) {
    return newAttachment(complexValueFk, null, null, null, data, null, null);
  }

  public static Attachment newAttachment(
      String url, String name, String type, byte[] data,
      Date timeStamp, Integer flags)
  {
    return newAttachment(INVALID_COMPLEX_VALUE_ID, url, name, type, data,
                         timeStamp, flags);
  }
  
  public static Attachment newAttachment(
      ComplexValueForeignKey complexValueFk, String url, String name,
      String type, byte[] data, Date timeStamp, Integer flags)
  {
    return new AttachmentImpl(INVALID_ID, complexValueFk, url, name, type,
                              data, timeStamp, flags, null);
  }

  public static Attachment newDecodedAttachment(byte[] decodedData) {
    return newDecodedAttachment(INVALID_COMPLEX_VALUE_ID, decodedData);
  }
  
  public static Attachment newDecodedAttachment(
      ComplexValueForeignKey complexValueFk, byte[] decodedData) {
    return newDecodedAttachment(complexValueFk, null, null, null, decodedData,
                                null, null);
  }

  public static Attachment newDecodedAttachment(
      String url, String name, String type, byte[] decodedData,
      Date timeStamp, Integer flags)
  {
    return newDecodedAttachment(INVALID_COMPLEX_VALUE_ID, url, name, type, 
                                decodedData, timeStamp, flags);
  }
  
  public static Attachment newDecodedAttachment(
      ComplexValueForeignKey complexValueFk, String url, String name,
      String type, byte[] decodedData, Date timeStamp, Integer flags)
  {
    return new AttachmentImpl(INVALID_ID, complexValueFk, url, name, type,
                              null, timeStamp, flags, decodedData);
  }

  
  public static boolean isAttachmentColumn(Table typeObjTable) {
    // attachment data has these columns FileURL(MEMO), FileName(TEXT),
    // FileType(TEXT), FileData(OLE), FileTimeStamp(SHORT_DATE_TIME),
    // FileFlags(LONG)
    List<Column> typeCols = typeObjTable.getColumns();
    if(typeCols.size() < 6) {
      return false;
    }

    int numMemo = 0;
    int numText = 0;
    int numDate = 0;
    int numOle= 0;
    int numLong = 0;
    
    for(Column col : typeCols) {
      switch(col.getType()) {
      case TEXT:
        ++numText;
        break;
      case LONG:
        ++numLong;
        break;
      case SHORT_DATE_TIME:
        ++numDate;
        break;
      case OLE:
        ++numOle;
        break;
      case MEMO:
        ++numMemo;
        break;
      default:
        // ignore
      }
    }

    // be flexible, allow for extra columns...
    return((numMemo >= 1) && (numText >= 2) && (numOle >= 1) &&
           (numDate >= 1) && (numLong >= 1));
  }


  private static class AttachmentImpl extends ComplexValueImpl
    implements Attachment
  {
    private String _url;
    private String _name;
    private String _type;
    private byte[] _data;
    private Date _timeStamp;
    private Integer _flags;
    private byte[] _decodedData;

    private AttachmentImpl(int id, ComplexValueForeignKey complexValueFk,
                           String url, String name, String type, byte[] data,
                           Date timeStamp, Integer flags, byte[] decodedData)
    {
      super(id, complexValueFk);
      _url = url;
      _name = name;
      _type = type;
      _data = data;
      _timeStamp = timeStamp;
      _flags = flags;
      _decodedData = decodedData;
    }
    
    public byte[] getFileData() {
      if((_data == null) && (_decodedData != null)) {
        _data = encodeData();
      }
      return _data;
    }

    public void setFileData(byte[] data) {
      _data = data;
      _decodedData = null;
    }

    public byte[] getDecodedFileData() throws IOException {
      if((_decodedData == null) && (_data != null)) {
        _decodedData = decodeData();
      }
      return _decodedData;
    }

    public void setDecodedFileData(byte[] data) {
      _decodedData = data;
      _data = null;
    }

    public String getFileName() {
      return _name;
    }

    public void setFileName(String fileName) {
      _name = fileName;
    }
  
    public String getFileUrl() {
      return _url;
    }

    public void setFileUrl(String fileUrl) {
      _url = fileUrl;
    }
  
    public String getFileType() {
      return _type;
    }

    public void setFileType(String fileType) {
      _type = fileType;
    }
  
    public Date getFileTimeStamp() {
      return _timeStamp;
    }

    public void setFileTimeStamp(Date fileTimeStamp) {
      _timeStamp = fileTimeStamp;
    }
  
    public Integer getFileFlags() {
      return _flags;
    }

    public void setFileFlags(Integer fileFlags) {
      _flags = fileFlags;
    }  

    public void update() throws IOException {
      getComplexValueForeignKey().updateAttachment(this);
    }
    
    public void delete() throws IOException {
      getComplexValueForeignKey().deleteAttachment(this);
    }
    
    @Override
    public String toString()
    {
      return "Attachment(" + getComplexValueForeignKey() + "," + getId() +
        ") " + getFileUrl() + ", " + getFileName() + ", " + getFileType()
        + ", " + getFileTimeStamp() + ", " + getFileFlags()  + ", " +
        ByteUtil.toHexString(getFileData());
    } 

    /**
     * Decodes the raw attachment file data to get the _actual_ content.
     */
    private byte[] decodeData() throws IOException {

      if(_data.length < 8) {
        // nothing we can do
        throw new IOException("Unknown encoded attachment data format");
      }

      // read initial header info
      ByteBuffer bb = PageChannel.wrap(_data);
      int typeFlag = bb.getInt();
      int dataLen = bb.getInt();

      DataInputStream contentStream = null;
      try {
        InputStream bin = new ByteArrayInputStream(
            _data, 8, _data.length - 8);

        if(typeFlag == DATA_TYPE_RAW) {
          // nothing else to do
        } else if(typeFlag == DATA_TYPE_COMPRESSED) {
          // actual content is deflate compressed
          bin = new InflaterInputStream(bin);
        } else {
          throw new IOException(
              "Unknown encoded attachment data type " + typeFlag);
        }

        contentStream = new DataInputStream(bin);

        // header is the "file extension" of the data.  no clue why we need
        // that again since it's already a separate field in the attachment
        // table.  just skip it
        byte[] tmpBytes = new byte[4];
        contentStream.readFully(tmpBytes);
        int headerLen = PageChannel.wrap(tmpBytes).getInt();
        contentStream.skipBytes(headerLen - 4);

        // calculate actual data length and read it (note, header length
        // includes the bytes for the length)
        tmpBytes = new byte[dataLen - headerLen];
        contentStream.readFully(tmpBytes);

        return tmpBytes;

      } finally {
        if(contentStream != null) {
          try {
            contentStream.close();
          } catch(IOException e) {
            // ignored
          }
        }
      }
    }

    /**
     * Encodes the actual attachment file data to get the raw, stored format.
     */
    private byte[] encodeData() {
      // FIXME, writeme
      throw new UnsupportedOperationException();
    }
  }

}
