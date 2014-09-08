/*
Copyright (c) 2014 James Ahlborn

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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * Utility code for dealing with calculated columns.
 * <p/>
 * These are the currently possible calculated types: FLOAT, DOUBLE, INT,
 * LONG, GUID, SHORT_DATE_TIME, MONEY, BOOLEAN, NUMERIC, TEXT, MEMO.
 *
 * @author James Ahlborn
 */
class CalculatedColumnUtil 
{
  // offset to the int which specifies the length of the actual data
  private static final int CALC_DATA_LEN_OFFSET = 16;
  // offset to the actual data
  private static final int CALC_DATA_OFFSET = CALC_DATA_LEN_OFFSET + 4;
  // total amount of extra bytes added for calculated values
  private static final int CALC_EXTRA_DATA_LEN = 23;

  // fully encode calculated BOOLEAN "true" value
  private static final byte[] CALC_BOOL_TRUE = wrapCalculatedValue(
      new byte[]{(byte)0xFF});
  // fully encode calculated BOOLEAN "false" value
  private static final byte[] CALC_BOOL_FALSE = wrapCalculatedValue(
      new byte[]{0});

  /**
   * Creates the appropriate ColumnImpl class for a calculated column and
   * reads a column definition in from a buffer
   * 
   * @param table owning table
   * @param buffer Buffer containing column definition
   * @param offset Offset in the buffer at which the column definition starts
   * @usage _advanced_method_
   */
  static ColumnImpl create(ColumnImpl.InitArgs args) throws IOException
  {    
    switch(args.type) {
    case BOOLEAN:
      return new CalcBooleanColImpl(args);
    case TEXT:
      return new CalcTextColImpl(args);
    case MEMO:
      return new CalcMemoColImpl(args);
    default:
      // fall through
    }

    if(args.type.getHasScalePrecision()) {
      return new CalcNumericColImpl(args);
    }
    
    return new CalcColImpl(args);
  }

  /**
   * Grabs the real data bytes from a calculated value.
   */
  private static byte[] unwrapCalculatedValue(byte[] data) {
    if(data.length < CALC_DATA_OFFSET) {
      return data;
    }
    
    ByteBuffer buffer = PageChannel.wrap(data);
    buffer.position(CALC_DATA_LEN_OFFSET);
    int dataLen = buffer.getInt();
    byte[] newData = new byte[Math.min(buffer.remaining(), dataLen)];
    buffer.get(newData);
    return newData;
  }

  /**
   * Wraps the given data bytes with the extra calculated value data and
   * returns a new ByteBuffer containing the final data.
   */
  private static ByteBuffer wrapCalculatedValue(ByteBuffer buffer) {
    ByteBuffer newBuf = prepareWrappedCalcValue(
        buffer.remaining(), buffer.order());
    newBuf.put(buffer);
    newBuf.rewind();
    return newBuf;
  }

  /**
   * Wraps the given data bytes with the extra calculated value data and
   * returns a new byte[] containing the final data.
   */
  private static byte[] wrapCalculatedValue(byte[] data) {
    int dataLen = data.length;
    data = ByteUtil.copyOf(data, 0, dataLen + CALC_EXTRA_DATA_LEN, 
                           CALC_DATA_OFFSET);
    PageChannel.wrap(data).putInt(CALC_DATA_LEN_OFFSET, dataLen);
    return data;
  }

  /**
   * Prepares a calculated value buffer for data of the given length.
   */
  private static ByteBuffer prepareWrappedCalcValue(int dataLen, ByteOrder order)
  {
    ByteBuffer buffer = ByteBuffer.allocate(
        dataLen + CALC_EXTRA_DATA_LEN).order(order);
    buffer.putInt(CALC_DATA_LEN_OFFSET, dataLen);
    buffer.position(CALC_DATA_OFFSET);
    return buffer;
  }
  

  /**
   * General calculated column implementation.
   */
  private static class CalcColImpl extends ColumnImpl
  {
    CalcColImpl(InitArgs args) throws IOException {
      super(args);
    }

    @Override
    public Object read(byte[] data, ByteOrder order) throws IOException {
      return super.read(unwrapCalculatedValue(data), order);
    }

    @Override
    protected ByteBuffer writeRealData(Object obj, int remainingRowLength, 
                                       ByteOrder order)
      throws IOException
    {
      // we should only be working with fixed length types
      ByteBuffer buffer = writeFixedLengthField(
          obj, prepareWrappedCalcValue(getType().getFixedSize(), order));
      buffer.rewind();
      return buffer;
    }
  }

  /**
   * Calculated BOOLEAN column implementation.
   */
  private static class CalcBooleanColImpl extends ColumnImpl
  {
    CalcBooleanColImpl(InitArgs args) throws IOException {
      super(args);
    }

    @Override
    public boolean storeInNullMask() {
      // calculated booleans are _not_ stored in null mask
      return false;
    }

    @Override
    public Object read(byte[] data, ByteOrder order) throws IOException {
      data = unwrapCalculatedValue(data);
      return ((data[0] != 0) ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    protected ByteBuffer writeRealData(Object obj, int remainingRowLength, 
                                       ByteOrder order)
      throws IOException
    {
      return ByteBuffer.wrap(
          toBooleanValue(obj) ? CALC_BOOL_TRUE : CALC_BOOL_FALSE).order(order);
    }
  }

  /**
   * Calculated TEXT column implementation.
   */
  private static class CalcTextColImpl extends TextColumnImpl
  {
    CalcTextColImpl(InitArgs args) throws IOException {
      super(args);
    }

    @Override
    public short getLengthInUnits() {
      // the byte "length" includes the calculated field overhead.  remove
      // that to get the _actual_ data length (in units)
      return (short)getType().toUnitSize(getLength() - CALC_EXTRA_DATA_LEN);
    }

    @Override
    public Object read(byte[] data, ByteOrder order) throws IOException {
      return decodeTextValue(unwrapCalculatedValue(data));
    }

    @Override
    protected ByteBuffer writeRealData(Object obj, int remainingRowLength, 
                                       ByteOrder order)
      throws IOException
    {
      return wrapCalculatedValue(super.writeRealData(
                                     obj, remainingRowLength, order));
    }
  }

  /**
   * Calculated MEMO column implementation.
   */
  private static class CalcMemoColImpl extends MemoColumnImpl
  {
    CalcMemoColImpl(InitArgs args) throws IOException {
      super(args);
    }

    @Override
    protected int getMaxLengthInUnits() {
      // the byte "length" includes the calculated field overhead.  remove
      // that to get the _actual_ data length (in units)
      return getType().toUnitSize(getType().getMaxSize() - CALC_EXTRA_DATA_LEN);
    }

    @Override
    protected byte[] readLongValue(byte[] lvalDefinition)
      throws IOException
    {
      return unwrapCalculatedValue(super.readLongValue(lvalDefinition));
    }

    @Override
    protected ByteBuffer writeLongValue(byte[] value, int remainingRowLength) 
      throws IOException
    {
      return super.writeLongValue(
          wrapCalculatedValue(value), remainingRowLength);
    }    
  }

  /**
   * Calculated NUMERIC column implementation.
   */
  private static class CalcNumericColImpl extends NumericColumnImpl
  {
    CalcNumericColImpl(InitArgs args) throws IOException {
      super(args);
    }

    @Override
    public byte getPrecision() {
      return (byte)getType().getMaxPrecision();
    }

    @Override
    public Object read(byte[] data, ByteOrder order) throws IOException {
      data = unwrapCalculatedValue(data);
      return readCalcNumericValue(ByteBuffer.wrap(data).order(order));
    }

    @Override
    protected ByteBuffer writeRealData(Object obj, int remainingRowLength, 
                                       ByteOrder order)
      throws IOException
    {
      int totalDataLen = Math.min(CALC_EXTRA_DATA_LEN + 16 + 4, getLength());
      int dataLen = totalDataLen - CALC_EXTRA_DATA_LEN;
      ByteBuffer buffer = prepareWrappedCalcValue(dataLen, order);

      writeCalcNumericValue(buffer, obj, dataLen);

      buffer.rewind();
      return buffer;
    }

    private static BigDecimal readCalcNumericValue(ByteBuffer buffer)
    {
      short totalLen = buffer.getShort();
      // numeric bytes need to be a multiple of 4 and we currently handle at
      // most 16 bytes
      int numByteLen = ((totalLen > 0) ? totalLen : buffer.remaining()) - 2;
      numByteLen = Math.min((numByteLen / 4) * 4, 16);

      byte scale = buffer.get();
      boolean negate = (buffer.get() != 0);
      byte[] tmpArr = ByteUtil.getBytes(buffer, numByteLen);

      if(buffer.order() != ByteOrder.BIG_ENDIAN) {
        fixNumericByteOrder(tmpArr);
      }

      return toBigDecimal(tmpArr, negate, scale);
    }

    private void writeCalcNumericValue(ByteBuffer buffer, Object value,
                                       int dataLen)
      throws IOException
    {
      Object inValue = value;
      try {
        BigDecimal decVal = toBigDecimal(value);
        inValue = decVal;

        int signum = decVal.signum();
        if(signum < 0) {
          decVal = decVal.negate();
        }

        int maxScale = getType().getMaxScale();
        if(decVal.scale() > maxScale) {
          // adjust scale according to max (will cause the an
          // ArithmeticException if number has too many decimal places)
          decVal = decVal.setScale(maxScale);
        }
        int scale = decVal.scale();
        
        // check precision
        if(decVal.precision() > getType().getMaxPrecision()) {
          throw new IOException(
              "Numeric value is too big for specified precision "
              + getType().getMaxPrecision() + ": " + decVal);
        }
    
        // convert to unscaled BigInteger, big-endian bytes
        byte[] intValBytes = toUnscaledByteArray(decVal, dataLen - 4);

        if(buffer.order() != ByteOrder.BIG_ENDIAN) {
          fixNumericByteOrder(intValBytes);
        }

        buffer.putShort((short)(dataLen - 2));
        buffer.put((byte)scale);
        // write sign byte
        buffer.put((signum < 0) ? NUMERIC_NEGATIVE_BYTE : 0);
        buffer.put(intValBytes);

      } catch(ArithmeticException e) {
        throw (IOException)
          new IOException("Numeric value '" + inValue + "' out of range")
          .initCause(e);
      }
    }

    private static void fixNumericByteOrder(byte[] bytes) {

      // this is a little weird.  it looks like they decided to truncate
      // leading 0 bytes and _then_ swap endian, which ends up kind of odd.
      int pos = 0;
      if((bytes.length % 8) != 0) {
        // leading 4 bytes are swapped
        ByteUtil.swap4Bytes(bytes, 0);
        pos += 4;
      }

      // then fix endianness of each 8 byte segment
      for(; pos < bytes.length; pos+=8) {
        ByteUtil.swap8Bytes(bytes, pos);
      }
    }
  }

}
