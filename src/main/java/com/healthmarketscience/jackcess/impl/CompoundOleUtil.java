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

package com.healthmarketscience.jackcess.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.healthmarketscience.jackcess.impl.OleUtil.*;
import com.healthmarketscience.jackcess.util.MemFileChannel;
import static com.healthmarketscience.jackcess.util.OleBlob.*;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;

/**
 * Utility code for working with OLE data which is in the compound storage
 * format.  This functionality relies on the optional POI library.
 * <p/> 
 * Note that all POI usage is restricted to this file so that the basic ole
 * support in OleUtil can be utilized without requiring POI.
 *
 * @author James Ahlborn
 */
public class CompoundOleUtil implements OleUtil.CompoundPackageFactory
{
  private static final String ENTRY_NAME_CHARSET = "UTF-8";
  private static final String ENTRY_SEPARATOR = "/";
  private static final String CONTENTS_ENTRY = "CONTENTS";

  public CompoundOleUtil() 
  {
  }

  public ContentImpl createCompoundPackageContent(
      OleBlobImpl blob, String prettyName, String className, String typeName,
      ByteBuffer blobBb, int dataBlockLen)
  {
    return new CompoundContentImpl(blob, prettyName, className, typeName,
                                   blobBb.position(), dataBlockLen);
  }

  private static String encodeEntryName(String name) {
    try {
      return URLEncoder.encode(name, ENTRY_NAME_CHARSET);
    } catch(UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String decodeEntryName(String name) {
    try {
      return URLDecoder.decode(name, ENTRY_NAME_CHARSET);
    } catch(UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class CompoundContentImpl 
    extends EmbeddedPackageContentImpl
    implements CompoundContent
  {
    private NPOIFSFileSystem _fs;

    private CompoundContentImpl(
        OleBlobImpl blob, String prettyName, String className,
        String typeName, int position, int length) 
    {
      super(blob, prettyName, className, typeName, position, length);
    }        

    public ContentType getType() {
      return ContentType.COMPOUND_STORAGE;
    }

    private NPOIFSFileSystem getFileSystem() throws IOException {
      if(_fs == null) {
        _fs = new NPOIFSFileSystem(MemFileChannel.newChannel(getStream(), "r"));
      }
      return _fs;
    }

    public List<String> getEntries() throws IOException {
      return getEntries(new ArrayList<String>(), getFileSystem().getRoot(),
                        ENTRY_SEPARATOR, false);
    }

    public InputStream getEntryStream(String entryName) throws IOException {
      return new DocumentInputStream(getDocumentEntry(entryName));
    }

    public boolean hasContentsEntry() throws IOException {
      return getFileSystem().getRoot().hasEntry(CONTENTS_ENTRY);
    }

    public InputStream getContentsEntryStream() throws IOException {
      return getEntryStream(CONTENTS_ENTRY);
    }

    private DocumentEntry getDocumentEntry(String entryName) throws IOException {

      // split entry name into individual components and decode them
      List<String> entryNames = new ArrayList<String>();
      for(String str : entryName.split(ENTRY_SEPARATOR)) {
        if(str.length() == 0) {
          continue;
        }
        entryNames.add(decodeEntryName(str));
      }

      DirectoryEntry dir = getFileSystem().getRoot();
      DocumentEntry entry = null;
      Iterator<String> iter = entryNames.iterator();
      while(iter.hasNext()) {
        Entry tmpEntry = dir.getEntry(iter.next());
        if(tmpEntry instanceof DirectoryEntry) {
          dir = (DirectoryEntry)tmpEntry;
        } else if(!iter.hasNext() && (tmpEntry instanceof DocumentEntry)) {
          entry = (DocumentEntry)tmpEntry;
        } else {
          break;
        }        
      }
      
      if(entry == null) {
        throw new FileNotFoundException("Could not find document " + entryName);
      }

      return entry;
    }

    private List<String> getEntries(List<String> entries, DirectoryEntry dir, 
                                    String prefix, boolean includeDetails) {
      for(Entry entry : dir) {
        if (entry instanceof DirectoryEntry) {
          // .. recurse into this directory
          getEntries(entries, (DirectoryEntry)entry, prefix + ENTRY_SEPARATOR,
                     includeDetails);
        } else if(entry instanceof DocumentEntry) {
          // grab the entry name/detils
          String entryName = prefix + encodeEntryName(entry.getName());
          if(includeDetails) {
            entryName += " (" + ((DocumentEntry)entry).getSize() + ")";
          }
          entries.add(entryName);
        }
      }
      return entries;
    }

    @Override
    public void close() {
      ByteUtil.closeQuietly(_fs);
      _fs = null;
      super.close();
    }

    @Override
    public String toString() {
      ToStringBuilder sb = toString(CustomToStringStyle.builder(this));

      try {
        sb.append("hasContentsEntry", hasContentsEntry());
        sb.append("entries",
                  getEntries(new ArrayList<String>(), getFileSystem().getRoot(),
                             ENTRY_SEPARATOR, true));
      } catch(IOException e) {  
        sb.append("entries", "<" + e + ">");
      }

      return sb.toString();
    }
  }

}
