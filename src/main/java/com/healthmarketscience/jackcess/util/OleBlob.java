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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.util.List;

import com.healthmarketscience.jackcess.impl.OleUtil;

/**
 *
 * Note, implementation is read-only. 
 *
 * @author James Ahlborn
 */
public interface OleBlob extends Blob, Closeable
{
  /** Enum describing the types of blob contents which are currently
      supported/understood */
  public enum ContentType {
    /** the blob contents are a link (file path) to some external content.
        Content will be an instance LinkContent */
    LINK, 
    /** the blob contents are a simple wrapper around some embedded content
        and related file names/paths.  Content will be an instance
        SimplePackageContent */
    SIMPLE_PACKAGE, 
    /** the blob contents are a complex embedded data known as compound
        storage (aka OLE2).  Working with compound storage requires the
        optional POI library Content will be an instance CompoundContent.  If
        the POI library is not available on the classpath, then compound
        storage data will instead be returned as type {@link #OTHER}. */
    COMPOUND_STORAGE,
    /** the top-level blob wrapper is understood, but the nested blob contents
        are unknown, probably just some embedded content.  Content will be an
        instance of OtherContent */
    OTHER,
    /** the top-level blob wrapper is not understood (this may not be a valid
        ole instance).  Content will simply be an instance of Content (the
        data can be accessed from the main blob instance) */ 
    UNKNOWN;
  }

  /**
   * Writes the entire raw blob data to the given stream (this is the access
   * db internal format, which includes all wrapper information).
   *
   * @param out stream to which the blob will be written
   */
  public void writeTo(OutputStream out) throws IOException;

  /**
   * Returns the decoded form of the blob contents, if understandable.
   */
  public Content getContent() throws IOException;


  public interface Content 
  {    
    /**
     * Returns the type of this content.
     */
    public ContentType getType();

    /**
     * Returns the blob which owns this content.
     */
    public OleBlob getBlob();
  }

  public interface PackageContent extends Content
  {    
    public String getPrettyName() throws IOException;

    public String getClassName() throws IOException;

    public String getTypeName() throws IOException;
  }

  public interface EmbeddedContent extends Content
  {
    public long length();

    public InputStream getStream() throws IOException;

    public void writeTo(OutputStream out) throws IOException;    
  }

  public interface LinkContent extends PackageContent
  {
    public String getFileName();

    public String getLinkPath();

    public String getFilePath();

    public InputStream getLinkStream() throws IOException;
  }

  public interface SimplePackageContent 
    extends PackageContent, EmbeddedContent
  {
    public String getFileName();

    public String getFilePath();

    public String getLocalFilePath();
  }

  public interface CompoundContent extends PackageContent, EmbeddedContent
  {
    public List<String> getEntries() throws IOException;

    public InputStream getEntryStream(String entryName) throws IOException;

    public boolean hasContentsEntry() throws IOException;

    public InputStream getContentsEntryStream() throws IOException;
  }  

  public interface OtherContent extends PackageContent, EmbeddedContent
  {
  }

  /**
   * Builder style class for constructing an OleBlob. The {@link
   * #fromInternalData} method can be used for interpreting existing ole data.
   * <p/>
   * Example for creating new ole data:
   * <pre>
   *   OleBlob oleBlob = new OleBlob.Builder()
   *     .setSimplePackageFile(contentFile)
   *     .toBlob();
   * </pre>
   */
  public class Builder
  {
    public static final String PACKAGE_PRETTY_NAME = "Packager Shell Object";
    public static final String PACKAGE_TYPE_NAME = "Package";

    private ContentType _type;
    private byte[] _bytes;
    private InputStream _stream;
    private long _contentLen;
    private String _fileName;
    private String _filePath;
    private String _prettyName;
    private String _className;
    private String _typeName;
    
    public ContentType getType() {
      return _type;
    }

    public byte[] getBytes() {
      return _bytes;
    }

    public InputStream getStream() {
      return _stream;
    }

    public long getContentLength() {
      return _contentLen;
    }

    public String getFileName() {
      return _fileName;
    }

    public String getFilePath() {
      return _filePath;
    }

    public String getPrettyName() {
      return _prettyName;
    }

    public String getClassName() {
      return _className;
    }
    
    public String getTypeName() {
      return _typeName;
    }
    
    public Builder setSimplePackageBytes(byte[] bytes) {
      _bytes = bytes;
      _contentLen = bytes.length;
      setDefaultPackageType();
      _type = ContentType.SIMPLE_PACKAGE;
      return this;
    }

    public Builder setSimplePackageStream(InputStream in, long length) {
      _stream = in;
      _contentLen = length;
      setDefaultPackageType();
      _type = ContentType.SIMPLE_PACKAGE;
      return this;
    }

    public Builder setSimplePackageFileName(String fileName) {
      _fileName = fileName;
      setDefaultPackageType();
      _type = ContentType.SIMPLE_PACKAGE;
      return this;
    }

    public Builder setSimplePackageFilePath(String filePath) {
      _filePath = filePath;
      setDefaultPackageType();
      _type = ContentType.SIMPLE_PACKAGE;
      return this;
    }

    public Builder setSimplePackage(File f) throws FileNotFoundException {
      _fileName = f.getName();
      _filePath = f.getPath();
      return setSimplePackageStream(new FileInputStream(f), f.length());
    }

    public Builder setLinkFileName(String fileName) {
      _fileName = fileName;
      setDefaultPackageType();
      _type = ContentType.LINK;
      return this;
    }

    public Builder setLinkPath(String link) {
      _filePath = link;
      setDefaultPackageType();
      _type = ContentType.LINK;
      return this;
    }

    public Builder setLink(File f) {
      _fileName = f.getName();
      _filePath = f.getPath();
      setDefaultPackageType();
      _type = ContentType.LINK;
      return this;
    }

    private void setDefaultPackageType() {
      if(_prettyName == null) {
        _prettyName = PACKAGE_PRETTY_NAME;
      }
      if(_className == null) {
        _className = PACKAGE_TYPE_NAME;
      }
    }

    public Builder setOtherBytes(byte[] bytes) {
      _bytes = bytes;
      _contentLen = bytes.length;
      _type = ContentType.OTHER;
      return this;
    }

    public Builder setOtherStream(InputStream in, long length) {
      _stream = in;
      _contentLen = length;
      _type = ContentType.OTHER;
      return this;
    }

    public Builder setPackagePrettyName(String prettyName) {
      _prettyName = prettyName;
      return this;
    }

    public Builder setPackageClassName(String className) {
      _className = className;
      return this;
    }

    public Builder setPackageTypeName(String typeName) {
      _typeName = typeName;
      return this;
    }

    public OleBlob toBlob() throws IOException {
      return OleUtil.createBlob(this);
    }

    public static OleBlob fromInternalData(byte[] bytes) throws IOException {
      return OleUtil.parseBlob(bytes);
    }
  }
}
