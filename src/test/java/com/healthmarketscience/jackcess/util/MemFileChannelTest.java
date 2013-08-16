/*
Copyright (c) 2012 James Ahlborn

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.util.Arrays;

import junit.framework.TestCase;

import com.healthmarketscience.jackcess.DatabaseTest;

/**
 *
 * @author James Ahlborn
 */
public class MemFileChannelTest extends TestCase 
{

  public MemFileChannelTest(String name) {
    super(name);
  }

  public void testReadOnlyChannel() throws Exception
  {
    File testFile = new File("src/test/data/V1997/compIndexTestV1997.mdb");
    MemFileChannel ch = MemFileChannel.newChannel(testFile, "r");
    assertEquals(testFile.length(), ch.size());
    assertEquals(0L, ch.position());

    try {
      ByteBuffer bb = ByteBuffer.allocate(1024);
      ch.write(bb);
      fail("NonWritableChannelException should have been thrown");
    } catch(NonWritableChannelException ignored) {
      // success
    }
    
    try {
      ch.truncate(0L);
      fail("NonWritableChannelException should have been thrown");
    } catch(NonWritableChannelException ignored) {
      // success
    }
    
    try {
      ch.transferFrom(null, 0L, 10L);
      fail("NonWritableChannelException should have been thrown");
    } catch(NonWritableChannelException ignored) {
      // success
    }

    assertEquals(testFile.length(), ch.size());
    assertEquals(0L, ch.position());

    ch.close();
  }

  public void testChannel() throws Exception
  {
    ByteBuffer bb = ByteBuffer.allocate(1024);

    MemFileChannel ch = MemFileChannel.newChannel();
    assertTrue(ch.isOpen());
    assertEquals(0L, ch.size());
    assertEquals(0L, ch.position());
    assertEquals(-1, ch.read(bb));
    
    ch.close();

    assertFalse(ch.isOpen());

    File testFile = new File("src/test/data/V1997/compIndexTestV1997.mdb");
    ch = MemFileChannel.newChannel(testFile, "r");
    assertEquals(testFile.length(), ch.size());
    assertEquals(0L, ch.position());

    try {
      ch.position(-1);
      fail("IllegalArgumentException should have been thrown");
    } catch(IllegalArgumentException ignored) {
      // success
    }
    
    MemFileChannel ch2 = MemFileChannel.newChannel();
    ch.transferTo(ch2);
    ch2.force(true);
    assertEquals(testFile.length(), ch2.size());
    assertEquals(testFile.length(), ch2.position());

    try {
      ch2.truncate(-1L);
      fail("IllegalArgumentException should have been thrown");
    } catch(IllegalArgumentException ignored) {
      // success
    }
    
    long trucSize = ch2.size()/3;
    ch2.truncate(trucSize);
    assertEquals(trucSize, ch2.size());
    assertEquals(trucSize, ch2.position());
    ch2.position(0L);
    copy(ch, ch2, bb);

    File tmpFile = File.createTempFile("chtest_", ".dat");
    tmpFile.deleteOnExit();
    FileOutputStream fc = new FileOutputStream(tmpFile);

    ch2.transferTo(fc);

    fc.close();

    assertEquals(testFile.length(), tmpFile.length());

    assertTrue(Arrays.equals(DatabaseTest.toByteArray(testFile),
                             DatabaseTest.toByteArray(tmpFile)));

    ch2.truncate(0L);
    assertTrue(ch2.isOpen());
    assertEquals(0L, ch2.size());
    assertEquals(0L, ch2.position());
    assertEquals(-1, ch2.read(bb));

    ch2.close();
    assertFalse(ch2.isOpen());
  }

  private static void copy(FileChannel src, FileChannel dst, ByteBuffer bb)
    throws IOException
  {
    src.position(0L);
    while(true) {
      bb.clear();
      if(src.read(bb) < 0) {
        break;
      }
      bb.flip();
      dst.write(bb);
    }
  }

}
