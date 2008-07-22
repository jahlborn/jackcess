package com.healthmarketscience.jackcess.scsu;

/*
 * This sample software accompanies Unicode Technical Report #6 and
 * distributed as is by Unicode, Inc., subject to the following:
 *
 * Copyright 1996-1998 Unicode, Inc.. All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software
 * without fee is hereby granted provided that this copyright notice
 * appears in all copies.
 *
 * UNICODE, INC. MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.
 * UNICODE, INC., SHALL NOT BE LIABLE FOR ANY ERRORS OR OMISSIONS, AND
 * SHALL NOT BE LIABLE FOR ANY DAMAGES, INCLUDING CONSEQUENTIAL AND
 * INCIDENTAL DAMAGES, SUFFERED BY YOU AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 *
 *  @author Asmus Freytag
 *
 *  @version 001 Dec 25 1996
 *  @version 002 Jun 25 1997
 *  @version 003 Jul 25 1997
 *  @version 004 Aug 25 1997
 *  @version 005 Sep 30 1998  
 *
 * Unicode and the Unicode logo are trademarks of Unicode, Inc.,
 * and are registered in some jurisdictions.
 **/

 /**
    Reference decoder for the Standard Compression Scheme for Unicode (SCSU)

    <H2>Notes on the Java implementation</H2>

    A limitation of Java is the exclusive use of a signed byte data type.
    The following work arounds are required:

    Copying a byte to an integer variable and adding 256 for 'negative'
    bytes gives an integer in the range 0-255.

    Values of char are between 0x0000 and 0xFFFF in Java. Arithmetic on
    char values is unsigned.

    Extended characters require an int to store them. The sign is not an
    issue because only 1024*1024 + 65536 extended characters exist.

**/
public class Expand extends SCSU
{
    /** (re-)define (and select) a dynamic window
    A sliding window position cannot start at any Unicode value,
    so rather than providing an absolute offset, this function takes
    an index value which selects among the possible starting values.

    Most scripts in Unicode start on or near a half-block boundary
    so the default behaviour is to multiply the index by 0x80. Han,
    Hangul, Surrogates and other scripts between 0x3400 and 0xDFFF
    show very poor locality--therefore no sliding window can be set
    there. A jumpOffset is added to the index value to skip that region,
    and only 167 index values total are required to select all eligible
    half-blocks.

    Finally, a few scripts straddle half block boundaries. For them, a
    table of fixed offsets is used, and the index values from 0xF9 to
    0xFF are used to select these special offsets.

    After (re-)defining a windows location it is selected so it is ready
    for use.

    Recall that all Windows are of the same length (128 code positions).

    @param iWindow - index of the window to be (re-)defined
    @param bOffset - index for the new offset value
    **/
	// @005 protected <-- private here and elsewhere
    protected void defineWindow(int iWindow, byte bOffset)
        throws IllegalInputException
    {
        int iOffset = (bOffset < 0 ? bOffset + 256 : bOffset);

        // 0 is a reserved value
        if (iOffset == 0)
        {
            throw new IllegalInputException();
        }
        else if (iOffset < gapThreshold)
        {
            dynamicOffset[iWindow] = iOffset << 7;
        }
        else if (iOffset < reservedStart)
        {
            dynamicOffset[iWindow] = (iOffset << 7) + gapOffset;
        }
        else if (iOffset < fixedThreshold)
        {
            // more reserved values
            throw new IllegalInputException("iOffset == "+iOffset);
        }
        else
        {
            dynamicOffset[iWindow] = fixedOffset[iOffset - fixedThreshold];
        }

        // make the redefined window the active one
        selectWindow(iWindow);
    }

    /** (re-)define (and select) a window as an extended dynamic window
    The surrogate area in Unicode allows access to 2**20 codes beyond the
    first 64K codes by combining one of 1024 characters from the High
    Surrogate Area with one of 1024 characters from the Low Surrogate
    Area (see Unicode 2.0 for the details).

    The tags SDX and UDX set the window such that each subsequent byte in
    the range 80 to FF represents a surrogate pair. The following diagram
    shows how the bits in the two bytes following the SDX or UDX, and a
    subsequent data byte, map onto the bits in the resulting surrogate pair.

     hbyte         lbyte          data
    nnnwwwww      zzzzzyyy      1xxxxxxx

     high-surrogate     low-surrogate
    110110wwwwwzzzzz   110111yyyxxxxxxx

    @param chOffset - Since the three top bits of chOffset are not needed to
    set the location of the extended Window, they are used instead
    to select the window, thereby reducing the number of needed command codes.
    The bottom 13 bits of chOffset are used to calculate the offset relative to
    a 7 bit input data byte to yield the 20 bits expressed by each surrogate pair.
    **/
    protected void defineExtendedWindow(char chOffset)
    {
        // The top 3 bits of iOffsetHi are the window index
        int iWindow = chOffset >>> 13;

        // Calculate the new offset
        dynamicOffset[iWindow] = ((chOffset & 0x1FFF) << 7) + (1 << 16);

        // make the redefined window the active one
        selectWindow(iWindow);
    }

    /** string buffer length used by the following functions */
    protected int iOut = 0;

    /** input cursor used by the following functions */
    protected int iIn = 0;

    /** expand input that is in Unicode mode
    @param in input byte array to be expanded
    @param iCur starting index
    @param sb string buffer to which to append expanded input
    @return the index for the lastc byte processed
    **/
    protected int expandUnicode(byte []in, int iCur, StringBuilder sb)
        throws IllegalInputException, EndOfInputException
    {
        for( ; iCur < in.length-1; iCur+=2 ) // step by 2:
        {
            byte b = in[iCur];

            if (b >= UC0 && b <= UC7)
            {
                Debug.out("SelectWindow: ", b);
                selectWindow(b - UC0);
                return iCur;
            }
            else if (b >= UD0 && b <= UD7)
            {
                defineWindow( b - UD0, in[iCur+1]);
                return iCur + 1;
            }
            else if (b == UDX)
            {
                if( iCur >= in.length - 2)
                {
                    break; // buffer error
                }
                defineExtendedWindow(charFromTwoBytes(in[iCur+1], in[iCur+2]));
                return iCur + 2;
            }
            else if (b == UQU)
            {
                if( iCur >= in.length - 2)
                {
                    break; // error
                }
                // Skip command byte and output Unicode character
                iCur++;
            }

            // output a Unicode character
            char ch = charFromTwoBytes(in[iCur], in[iCur+1]);
            sb.append(ch);
            iOut++;
        }

        if( iCur == in.length)
        {
            return iCur;
        }

        // Error condition
        throw new EndOfInputException();
    }

    /** assemble a char from two bytes
    In Java bytes are signed quantities, while chars are unsigned
    @return the character
    @param hi most significant byte
    @param lo least significant byte
    */
    public static char charFromTwoBytes(byte hi, byte lo)
    {
        char ch = (char)(lo >= 0 ? lo : 256 + lo);
        return (char)(ch + (char)((hi >= 0 ? hi : 256 + hi)<<8));
    }

    /** expand portion of the input that is in single byte mode **/
    @SuppressWarnings("fallthrough")
    protected String expandSingleByte(byte []in)
        throws IllegalInputException, EndOfInputException
    {

        /* Allocate the output buffer. Because of control codes, generally
        each byte of input results in fewer than one character of
        output. Using in.length as an intial allocation length should avoid
        the need to reallocate in mid-stream. The exception to this rule are
        surrogates. */
        StringBuilder sb = new StringBuilder(in.length);
        iOut = 0;

        // Loop until all input is exhausted or an error occurred
        int iCur;
        Loop:
        for( iCur = 0; iCur < in.length; iCur++ )
        {
            // DEBUG Debug.out("Expanding: ", iCur);

            // Default behaviour is that ASCII characters are passed through
            // (staticOffset[0] == 0) and characters with the high bit on are
            // offset by the current dynamic (or sliding) window (this.iWindow)
            int iStaticWindow = 0;
            int iDynamicWindow = getCurrentWindow();

            switch(in[iCur])
            {
                // Quote from a static Window
            case SQ0:
            case SQ1:
            case SQ2:
            case SQ3:
            case SQ4:
            case SQ5:
            case SQ6:
            case SQ7:
                Debug.out("SQn:", iStaticWindow);
                // skip the command byte and check for length
                if( iCur >= in.length - 1)
                {
                    Debug.out("SQn missing argument: ", in, iCur);
                    break Loop;  // buffer length error
                }
                // Select window pair to quote from
                iDynamicWindow = iStaticWindow = in[iCur] - SQ0;
                iCur ++;

                // FALL THROUGH

            default:
                // output as character
                if(in[iCur] >= 0)
                {
                    // use static window
                    int ch = in[iCur] + staticOffset[iStaticWindow];
                    sb.append((char)ch);
                    iOut++;
                }
                else
                {
                    // use dynamic window
                    int ch = (in[iCur] + 256); // adjust for signed bytes
                    ch -= 0x80;                // reduce to range 00..7F
                    ch += dynamicOffset[iDynamicWindow];

                    //DEBUG
                    Debug.out("Dynamic: ", (char) ch);

                    if (ch < 1<<16)
                    {
                        // in Unicode range, output directly
                        sb.append((char)ch);
                        iOut++;
                    }
                    else
                    {
                        // this is an extension character
                        Debug.out("Extension character: ", ch);

                        // compute and append the two surrogates:
                        // translate from 10000..10FFFF to 0..FFFFF
                        ch -= 0x10000;

                        // high surrogate = top 10 bits added to D800
                        sb.append((char)(0xD800 + (ch>>10)));
                        iOut++;

                        // low surrogate = bottom 10 bits added to DC00
                        sb.append((char)(0xDC00 + (ch & ~0xFC00)));
                        iOut++;
                    }
                }
                break;

                // define a dynamic window as extended
            case SDX:
                iCur += 2;
                if( iCur >= in.length)
                {
                    Debug.out("SDn missing argument: ", in, iCur -1);
                    break Loop;  // buffer length error
                }
                defineExtendedWindow(charFromTwoBytes(in[iCur-1], in[iCur]));
                break;

                // Position a dynamic Window
            case SD0:
            case SD1:
            case SD2:
            case SD3:
            case SD4:
            case SD5:
            case SD6:
            case SD7:
                iCur ++;
                if( iCur >= in.length)
                {
                    Debug.out("SDn missing argument: ", in, iCur -1);
                    break Loop;  // buffer length error
                }
                defineWindow(in[iCur-1] - SD0, in[iCur]);
                break;

                // Select a new dynamic Window
            case SC0:
            case SC1:
            case SC2:
            case SC3:
            case SC4:
            case SC5:
            case SC6:
            case SC7:
                selectWindow(in[iCur] - SC0);
                break;
            case SCU:
                // switch to Unicode mode and continue parsing
                iCur = expandUnicode(in, iCur+1, sb);
                // DEBUG Debug.out("Expanded Unicode range until: ", iCur);
                break;

            case SQU:
                // directly extract one Unicode character
                iCur += 2;
                if( iCur >= in.length)
                {
                     Debug.out("SQU missing argument: ", in, iCur - 2);
                     break Loop;  // buffer length error
                }
                else
                {
                    char ch = charFromTwoBytes(in[iCur-1], in[iCur]);

                    Debug.out("Quoted: ", ch);
                    sb.append(ch);
                    iOut++;
                }
                break;

             case Srs:
                throw new IllegalInputException();
                // break;
            }
        }

        if( iCur >= in.length)
        {
            //SUCCESS: all input used up
            sb.setLength(iOut);
            iIn = iCur;
            return sb.toString();
        }

        Debug.out("Length ==" + in.length+" iCur =", iCur);
        //ERROR: premature end of input
        throw new EndOfInputException();
    }

    /** expand a byte array containing compressed Unicode */
    public String expand (byte []in)
        throws IllegalInputException, EndOfInputException
    {
        String str = expandSingleByte(in);
        Debug.out("expand output: ", str.toCharArray());
        return str;
    }


    /** reset is called to start with new input, w/o creating a new
        instance */
    @Override
    public void reset()
    {
        iOut = 0;
        iIn = 0;
        super.reset();
    }

    public int charsWritten()
    {
        return iOut;
    }

    public int bytesRead()
    {
        return iIn;
    }
}
