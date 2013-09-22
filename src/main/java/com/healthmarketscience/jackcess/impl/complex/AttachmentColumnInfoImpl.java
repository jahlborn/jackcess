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

package com.healthmarketscience.jackcess.impl.complex;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.AttachmentColumnInfo;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.JetFormat;
import com.healthmarketscience.jackcess.impl.PageChannel;


/**
 * Complex column info for a column holding 0 or more attachments per row.
 *
 * @author James Ahlborn
 */
public class AttachmentColumnInfoImpl extends ComplexColumnInfoImpl<Attachment>
  implements AttachmentColumnInfo
{
  /** some file formats which may not be worth re-compressing */
  private static final Set<String> COMPRESSED_FORMATS = new HashSet<String>(
      Arrays.asList("jpg", "zip", "gz", "bz2", "z", "7z", "cab", "rar", 
                    "mp3", "mpg"));

  private static final String FILE_NAME_COL_NAME = "FileName";
  private static final String FILE_TYPE_COL_NAME = "FileType";

  private static final int DATA_TYPE_RAW = 0;
  private static final int DATA_TYPE_COMPRESSED = 1;

  private static final int UNKNOWN_HEADER_VAL = 1;
  private static final int WRAPPER_HEADER_SIZE = 8;
  private static final int CONTENT_HEADER_SIZE = 12;

  private final Column _fileUrlCol;
  private final Column _fileNameCol;
  private final Column _fileTypeCol;
  private final Column _fileDataCol;
  private final Column _fileTimeStampCol;
  private final Column _fileFlagsCol;
  
  public AttachmentColumnInfoImpl(Column column, int complexId,
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
                                   Row rawValue) {
    ComplexValue.Id id = getValueId(rawValue);
    String url = (String)getFileUrlColumn().getRowValue(rawValue);
    String name = (String)getFileNameColumn().getRowValue(rawValue);
    String type = (String)getFileTypeColumn().getRowValue(rawValue);
    Integer flags = (Integer)getFileFlagsColumn().getRowValue(rawValue);
    Date ts = (Date)getFileTimeStampColumn().getRowValue(rawValue);
    byte[] data = (byte[])getFileDataColumn().getRowValue(rawValue);
    
    return new AttachmentImpl(id, complexValueFk, url, name, type, null,
                              ts, flags, data);
  }

  @Override
  protected Object[] asRow(Object[] row, Attachment attachment) 
    throws IOException 
  {
    super.asRow(row, attachment);
    getFileUrlColumn().setRowValue(row, attachment.getFileUrl());
    getFileNameColumn().setRowValue(row, attachment.getFileName());
    getFileTypeColumn().setRowValue(row, attachment.getFileType());
    getFileFlagsColumn().setRowValue(row, attachment.getFileFlags());
    getFileTimeStampColumn().setRowValue(row, attachment.getFileTimeStamp());
    getFileDataColumn().setRowValue(row, attachment.getEncodedFileData());
    return row;
  }

  public static Attachment newAttachment(byte[] data) {
    return newAttachment(INVALID_FK, data);
  }
  
  public static Attachment newAttachment(ComplexValueForeignKey complexValueFk,
                                         byte[] data) {
    return newAttachment(complexValueFk, null, null, null, data, null, null);
  }

  public static Attachment newAttachment(
      String url, String name, String type, byte[] data,
      Date timeStamp, Integer flags)
  {
    return newAttachment(INVALID_FK, url, name, type, data,
                         timeStamp, flags);
  }
  
  public static Attachment newAttachment(
      ComplexValueForeignKey complexValueFk, String url, String name,
      String type, byte[] data, Date timeStamp, Integer flags)
  {
    return new AttachmentImpl(INVALID_ID, complexValueFk, url, name, type,
                              data, timeStamp, flags, null);
  }

  public static Attachment newEncodedAttachment(byte[] encodedData) {
    return newEncodedAttachment(INVALID_FK, encodedData);
  }
    
  public static Attachment newEncodedAttachment(
      ComplexValueForeignKey complexValueFk, byte[] encodedData) {
    return newEncodedAttachment(complexValueFk, null, null, null, encodedData,
                                null, null);
  }
 
  public static Attachment newEncodedAttachment(
      String url, String name, String type, byte[] encodedData,
      Date timeStamp, Integer flags)
  {
    return newEncodedAttachment(INVALID_FK, url, name, type, 
                                encodedData, timeStamp, flags);
  }
   
  public static Attachment newEncodedAttachment(
      ComplexValueForeignKey complexValueFk, String url, String name,
      String type, byte[] encodedData, Date timeStamp, Integer flags)
  {
    return new AttachmentImpl(INVALID_ID, complexValueFk, url, name, type,
                              null, timeStamp, flags, encodedData);
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
    private byte[] _encodedData;

    private AttachmentImpl(Id id, ComplexValueForeignKey complexValueFk,
                           String url, String name, String type, byte[] data,
                           Date timeStamp, Integer flags, byte[] encodedData)
    {
      super(id, complexValueFk);
      _url = url;
      _name = name;
      _type = type;
      _data = data;
      _timeStamp = timeStamp;
      _flags = flags;
      _encodedData = encodedData;
    }
    
    public byte[] getFileData() throws IOException {
      if((_data == null) && (_encodedData != null)) {
        _data = decodeData();
      }
      return _data;
    }

    public void setFileData(byte[] data) {
      _data = data;
      _encodedData = null;
    }

    public byte[] getEncodedFileData() throws IOException {
      if((_encodedData == null) && (_data != null)) {
        _encodedData = encodeData();
      }
      return _encodedData;
    }

    public void setEncodedFileData(byte[] data) {
      _encodedData = data;
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
    public String toString() {

      String dataStr = null;
      try {
        dataStr = ByteUtil.toHexString(getFileData());
      } catch(IOException e) {
        dataStr = e.toString();
      }
      
      return "Attachment(" + getComplexValueForeignKey() + "," + getId() +
        ") " + getFileUrl() + ", " + getFileName() + ", " + getFileType()
        + ", " + getFileTimeStamp() + ", " + getFileFlags()  + ", " +
        dataStr;
    } 

    /**
     * Decodes the raw attachment file data to get the _actual_ content.
     */
    private byte[] decodeData() throws IOException {

      if(_encodedData.length < WRAPPER_HEADER_SIZE) {
        // nothing we can do
        throw new IOException("Unknown encoded attachment data format");
      }
  
      // read initial header info
      ByteBuffer bb = PageChannel.wrap(_encodedData);
      int typeFlag = bb.getInt();
      int dataLen = bb.getInt();

      DataInputStream contentStream = null;
      try {
        InputStream bin = new ByteArrayInputStream(
            _encodedData, WRAPPER_HEADER_SIZE,
            _encodedData.length - WRAPPER_HEADER_SIZE);

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

        // header is an unknown flag followed by the "file extension" of the
        // data (no clue why we need that again since it's already a separate
        // field in the attachment table).  just skip all of it
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
        ByteUtil.closeQuietly(contentStream);
      }
    }

    /**
     * Encodes the actual attachment file data to get the raw, stored format.
     */
    private byte[] encodeData() throws IOException {

      // possibly compress data based on file type
      String type = ((_type != null) ? _type.toLowerCase() : "");
      boolean shouldCompress = !COMPRESSED_FORMATS.contains(type);

      // encode extension, which ends w/ a null byte
      type += '\0';
      ByteBuffer typeBytes = ColumnImpl.encodeUncompressedText(
          type, JetFormat.VERSION_12.CHARSET);
      int headerLen = typeBytes.remaining() + CONTENT_HEADER_SIZE;

      int dataLen = _data.length;
      ByteUtil.ByteStream dataStream = new ByteUtil.ByteStream(
          WRAPPER_HEADER_SIZE + headerLen + dataLen);

      // write the wrapper header info
      ByteBuffer bb = PageChannel.wrap(dataStream.getBytes());
      bb.putInt(shouldCompress ? DATA_TYPE_COMPRESSED : DATA_TYPE_RAW);
      bb.putInt(dataLen + headerLen);
      dataStream.skip(WRAPPER_HEADER_SIZE);

      OutputStream contentStream = dataStream;
      Deflater deflater = null;
      try {

        if(shouldCompress) {
          contentStream = new DeflaterOutputStream(
              contentStream, deflater = new Deflater(3));
        }

        // write the header w/ the file extension
        byte[] tmpBytes = new byte[CONTENT_HEADER_SIZE];
        PageChannel.wrap(tmpBytes)
          .putInt(headerLen)
          .putInt(UNKNOWN_HEADER_VAL)
          .putInt(type.length());
        contentStream.write(tmpBytes);
        contentStream.write(typeBytes.array(), 0, typeBytes.remaining());

        // write the _actual_ contents
        contentStream.write(_data);
        contentStream.close();
        contentStream = null;

        return dataStream.toByteArray();

      } finally {
        ByteUtil.closeQuietly(contentStream);
        if(deflater != null) {
          deflater.end();
        }
      }
    }
  }

}
