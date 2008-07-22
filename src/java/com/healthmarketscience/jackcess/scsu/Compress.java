package com.healthmarketscience.jackcess.scsu;

/**
 * This sample software accompanies Unicode Technical Report #6 and
 * distributed as is by Unicode, Inc., subject to the following:
 *
 * Copyright 1996-1997 Unicode, Inc.. All Rights Reserved.
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
 *
 * Unicode and the Unicode logo are trademarks of Unicode, Inc.,
 * and are registered in some jurisdictions.
 **/

/**
    This class implements a simple compression algorithm
 **/
/*
    Note on exception handling
        This compressor is designed so that it can be restarted after
        an exception. All operations advancing input and/or output cursor
        (iIn and iOut) either complete an action, or set a state (fUnicodeMode)
        before updating the cursors.
*/
public class Compress extends SCSU
{

    /** next input character to be read **/
    private int iIn;

    /** next output byte to be written **/
    private int iOut;

    /** start index of Unicode mode in output array, or -1 if in single byte mode **/
    private int iSCU = -1;

    /** true if the next command byte is of the Uxx family */
    private boolean fUnicodeMode = false;

    /** locate a window for a character given a table of offsets
    @param ch - character
    @param offsetTable - table of window offsets
    @return true if the character fits a window from the table of windows */
    private boolean locateWindow(int ch, int[] offsetTable)
    {
        // always try the current window first
        int iWin = getCurrentWindow();

        // if the character fits the current window
        // just use the current window
        if (iWin != - 1 && ch >= offsetTable[iWin] && ch < offsetTable[iWin] + 0x80)
        {
            return true;
        }

        // try all windows in order
        for (iWin = 0; iWin < offsetTable.length; iWin++)
        {
            if (ch >= offsetTable[iWin] && ch < offsetTable[iWin] + 0x80)
            {
                selectWindow(iWin);
                return true;
            }
        }
        // none found
        return false;
    }

    /** returns true if the character is ASCII, but not a control other than CR, LF and TAB */
    public static boolean isAsciiCrLfOrTab(int ch)
    {
        return    (ch >= 0x20 && ch <= 0x7F)                 // ASCII
                || ch == 0x09 || ch == 0x0A || ch == 0x0D;   // CR/LF or TAB

    }

    /** output a run of characters in single byte mode
        In single byte mode pass through characters in the ASCII range, but
        quote characters overlapping with compression command codes. Runs
        of characters fitting the current window are output as runs of bytes
        in the range 0x80-0xFF. Checks for and validates Surrogate Pairs.
        Uses and updates the current input and output cursors store in
        the instance variables <i>iIn</i> and <i>iOut</i>.
        @param in - input character array
        @param out - output byte array
        @return the next chaacter to be processed. This may be an extended character.
    **/
    @SuppressWarnings("fallthrough")
    public int outputSingleByteRun(char [] in, byte [] out)
        throws EndOfOutputException, EndOfInputException, IllegalInputException
    {
        int iWin = getCurrentWindow();
        while(iIn < in.length)
        {
            int outlen = 0;
            byte byte1 = 0;
            byte byte2 = 0;

            // get the input character
            int ch = in[iIn];

            int inlen = 1;

            // Check input for Surrogate pair
            if ( (ch & 0xF800) == 0xD800 )
            {
                if ( (ch & 0xFC00) == 0xDC00 )
                {
                    // low surrogate out of order
                    throw new IllegalInputException("Unpaired low surrogate: "+iIn);
                }
                else
                {
                    // have high surrogate now get low surrogate
                    if ( iIn >= in.length-1)
                    {
                        // premature end of input
                        throw new EndOfInputException();
                    }
                    // get the char
                    int ch2 = in[iIn+1];

                    // make sure it's a low surrogate
                    if ( (ch2 & 0xFC00) != 0xDC00 )
                    {
                        // a low surrogate was required
                        throw new IllegalInputException("Unpaired high surrogate: "+(iIn+1));
                    }

                    // combine the two values
                    ch = ((ch - 0xD800)<<10 | (ch2-0xDC00))+0x10000;
                    // ch = ch<<10 + ch2 - 0x36F0000;

                    inlen = 2;
                 }
            }

            // ASCII Letter, NUL, CR, LF and TAB are always passed through
            if (isAsciiCrLfOrTab(ch) || ch == 0)
            {
                // pass through directcly
                byte2 = (byte)(ch & 0x7F);
                outlen = 1;
            }

            // All other control codes must be quoted
            else if (ch < 0x20)
            {
                byte1 = SQ0;
                byte2 = (byte)(ch);
                outlen = 2;
            }

            // Letters that fit the current dynamic window
            else if (ch >= dynamicOffset[iWin] && ch < dynamicOffset[iWin] + 0x80)
            {
                ch -= dynamicOffset[iWin];
                byte2 = (byte)(ch | 0x80);
                outlen = 1;
            }

            // check for room in the output array
            if (iOut + outlen >= out.length)
            {
                throw new EndOfOutputException();
            }

            switch(outlen)
            {
                default:
                    // need to use some other compression mode for this
                    // character so we terminate this loop

                    return ch; // input not finished

                    // output the characters
                case 2:
                    out[iOut++] = byte1;
                    // fall through
                case 1:
                    out[iOut++] = byte2;
                    break;
            }
            // advance input pointer
            iIn += inlen;
        }
        return 0; // input all used up
    }

    /** quote a single character in single byte mode
    Quoting a character (aka 'non-locking shift') gives efficient access
    to characters that occur in isolation--usually punctuation characters.
    When quoting a character from a dynamic window use 0x80 - 0xFF, when
    quoting a character from a static window use 0x00-0x7f.
    @param ch - character to be quoted
    @param out - output byte array
    **/

    private void quoteSingleByte(int ch, byte [] out)
        throws EndOfOutputException
    {
        Debug.out("Quoting SingleByte ", ch);
        int iWin = getCurrentWindow();

        // check for room in the output array
        if (iOut >= out.length -2)
        {
            throw new EndOfOutputException();
        }

        // Output command byte followed by
        out[iOut++] = (byte)(SQ0 + iWin);

        // Letter that fits the current dynamic window
        if (ch >= dynamicOffset[iWin] && ch < dynamicOffset[iWin] + 0x80)
        {
            ch -= dynamicOffset[iWin];
            out[iOut++] = (byte)(ch | 0x80);
        }

        // Letter that fits the current static window
        else if (ch >= staticOffset[iWin] && ch < staticOffset[iWin] + 0x80)
        {
            ch -= staticOffset[iWin];
            out[iOut++] = (byte)ch;
        }
        else
        {
            throw new IllegalStateException("ch = "+ch+" not valid in quoteSingleByte. Internal Compressor Error");
        }
        // advance input pointer
        iIn ++;
        Debug.out("New input: ", iIn);
    }

    /** output a run of characters in Unicode mode
    A run of Unicode mode consists of characters which are all in the
    range of non-compressible characters or isolated occurrence
    of any other characters. Characters in the range 0xE00-0xF2FF must
    be quoted to avoid overlap with the Unicode mode compression command codes.
    Uses and updates the current input and output cursors store in
    the instance variables <i>iIn</i> and <i>iOut</i>.
    NOTE: Characters from surrogate pairs are passed through and unlike single
    byte mode no checks are made for unpaired surrogate characters.
    @param in - input character array
    @param out - output byte array
    @return the next input character to be processed
    **/
    public char outputUnicodeRun(char [] in, byte [] out)
        throws EndOfOutputException
    {
        // current character
        char ch = 0;

        while(iIn < in.length)
        {
            // get current input and set default output length
            ch = in[iIn];
            int outlen = 2;

            // Characters in these ranges could potentially be compressed.
            // We require 2 or more compressible characters to break the run
            if (isCompressible(ch))
            {
                // check whether we can look ahead
                if( iIn < in.length - 1)
                {
                    // DEBUG
                    Debug.out("is-comp: ",ch);
                    char ch2 = in[iIn + 1];
                    if (isCompressible(ch2))
                    {
                        // at least 2 characters are compressible
                        // break the run
                        break;
                    }
                    //DEBUG
                    Debug.out("no-comp: ",ch2);
                }
                // If we get here, the current character is only character
                // left in the input or it is followed by a non-compressible
                // character. In neither case do we gain by breaking the
                // run, so we proceed to output the character.
                if (ch >= 0xE000 && ch <= 0xF2FF)
                {
                    // Characters in this range need to be escaped
                    outlen = 3;
                }

            }
            // check that there is enough room to output the character
            if(iOut >= out.length - outlen)
            {
                // DEBUG
                Debug.out("End of Output @", iOut);
                // if we got here, we ran out of space in the output array
                throw new EndOfOutputException();
            }

            // output any characters that cannot be compressed,
            if (outlen == 3)
            {
                // output the quote character
                out[iOut++] = UQU;
            }
            // pass the Unicode character in MSB,LSB order
            out[iOut++] = (byte)(ch >>> 8);
            out[iOut++] = (byte)(ch & 0xFF);

            // advance input cursor
            iIn++;
        }

        // return the last character
        return ch;
    }

    static int iNextWindow = 3;

    /** redefine a window so it surrounds a given character value
        For now, this function uses window 3 exclusively (window 4
        for extended windows);
        @return true if a window was successfully defined
        @param ch - character around which window is positioned
        @param out - output byte array
        @param fCurUnicodeMode - type of window
     **/
    private boolean positionWindow(int ch, byte [] out, boolean fCurUnicodeMode)
        throws IllegalInputException, EndOfOutputException
    {
        int iWin = iNextWindow % 8; // simple LRU
        int iPosition = 0;

        // iPosition 0 is a reserved value
        if (ch < 0x80)
        {
            throw new IllegalStateException("ch < 0x80");
            //return false;
        }

        // Check the fixed offsets
        for (int i = 0; i < fixedOffset.length; i++)
        {
            if (ch >= fixedOffset[i] && ch < fixedOffset[i] + 0x80)
            {
                iPosition = i;
                break;
            }
        }

        if (iPosition != 0)
        {
            // DEBUG
            Debug.out("FIXED position is ", iPosition + 0xF9);

            // ch fits in a fixed offset window position
            dynamicOffset[iWin] = fixedOffset[iPosition];
            iPosition += 0xF9;
        }
        else if (ch < 0x3400)
        {
            // calculate a window position command and set the offset
            iPosition = ch >>> 7;
            dynamicOffset[iWin] = ch & 0xFF80;

            Debug.out("Offset="+dynamicOffset[iWin]+", iPosition="+iPosition+" for char", ch);
        }
        else if (ch < 0xE000)
        {
            // attempt to place a window where none can go
            return false;
        }
        else if (ch <= 0xFFFF)
        {
            // calculate a window position command, accounting
            // for the gap in position values, and set the offset
            iPosition =  ((ch - gapOffset)>>> 7);

            dynamicOffset[iWin] = ch & 0xFF80;

            Debug.out("Offset="+dynamicOffset[iWin]+", iPosition="+iPosition+" for char", ch);
        }
        else
        {
            // if we get here, the character is in the extended range.
            // Always use Window 4 to define an extended window

            iPosition = (ch - 0x10000) >>> 7;
            // DEBUG
            Debug.out("Try position Window at ", iPosition);

            iPosition |= iWin << 13;
            dynamicOffset[iWin] = ch & 0x1FFF80;
        }

        // Outputting window defintion command for the general cases
        if ( iPosition < 0x100 && iOut < out.length-1)
        {
            out[iOut++] = (byte) ((fCurUnicodeMode ? UD0 : SD0) + iWin);
            out[iOut++] = (byte) (iPosition & 0xFF);
        }
        // Output an extended window definiton command
        else if ( iPosition >= 0x100 && iOut < out.length - 2)
        {

            Debug.out("Setting extended window at ", iPosition);
            out[iOut++] = (fCurUnicodeMode ? UDX : SDX);
            out[iOut++] = (byte) ((iPosition >>> 8) & 0xFF);
            out[iOut++] = (byte) (iPosition & 0xFF);
        }
        else
        {
            throw new EndOfOutputException();
        }
        selectWindow(iWin);
        iNextWindow++;
        return true;
    }

    /**
    compress a Unicode character array with some simplifying assumptions
    **/
    public int simpleCompress(char [] in, int iStartIn, byte[] out, int iStartOut)
        throws IllegalInputException, EndOfInputException, EndOfOutputException
    {
        iIn = iStartIn;
        iOut = iStartOut;


        while (iIn < in.length)
        {
            int ch;

            // previously we switched to a Unicode run
            if (iSCU != -1)
            {

                Debug.out("Remaining", in, iIn);
                Debug.out("Output until ["+iOut+"]: ", out);

                // output characters as Unicode
                ch = outputUnicodeRun(in, out);

                // for single character Unicode runs (3 bytes) use quote
                if (iOut - iSCU == 3 )
                {
                    // go back and fix up the SCU to an SQU instead
                    out[iSCU] = SQU;
                    iSCU = -1;
                    continue;
                }
                else
                {
                    iSCU = -1;
                    fUnicodeMode = true;
                }
            }
            // next, try to output characters as single byte run
            else
            {
                ch = outputSingleByteRun(in, out);
            }

            // check whether we still have input
            if (iIn == in.length)
            {
                break; // no more input
            }

            // if we get here, we have a consistent value for ch, whether or
            // not it is an regular or extended character. Locate or define a
            // Window for the current character

            Debug.out("Output so far: ", out);
            Debug.out("Routing ch="+ch+" for Input", in, iIn);

            // Check that we have enough room to output the command byte
            if (iOut >= out.length - 1)
            {
                throw new EndOfOutputException();
            }

            // In order to switch away from Unicode mode, it is necessary
            // to select (or define) a window. If the characters that follow
            // the Unicode range are ASCII characters, we can't use them
            // to decide which window to select, since ASCII characters don't
            // influence window settings. This loop looks ahead until it finds
            // one compressible character that isn't in the ASCII range.
            for (int ich = iIn; ch < 0x80; ich++)
            {
                if (ich == in.length || !isCompressible(in[ich]))
                {
                    // if there are only ASCII characters left,
                    ch = in[iIn];
                    break;
                }
                ch = in[ich]; // lookahead for next non-ASCII char
            }
            // The character value contained in ch here will only be used to select
            // output modes. Actual output of characters starts with in[iIn] and
            // only takes place near the top of the loop.

            int iprevWindow = getCurrentWindow();

            // try to locate a dynamic window
            if (ch < 0x80 || locateWindow(ch, dynamicOffset))
            {
                Debug.out("located dynamic window "+getCurrentWindow()+" at ", iOut+1);
                // lookahead to use SQn instead of SCn for single
                // character interruptions of runs in current window
                if(!fUnicodeMode && iIn < in.length -1)
                {
                    char ch2 = in[iIn+1];
                    if (ch2 >= dynamicOffset[iprevWindow] &&
                        ch2 <  dynamicOffset[iprevWindow] + 0x80)
                    {
                        quoteSingleByte(ch, out);
                        selectWindow(iprevWindow);
                        continue;
                    }
                }

                out[iOut++] = (byte)((fUnicodeMode ? UC0 : SC0) + getCurrentWindow());
                fUnicodeMode = false;
            }
            // try to locate a static window
            else if (!fUnicodeMode && locateWindow(ch, staticOffset))
            {
                // static windows are not accessible from Unicode mode
                Debug.out("located a static window", getCurrentWindow());
                quoteSingleByte(ch, out);
                selectWindow(iprevWindow); // restore current Window settings
                continue;
            }
            // try to define a window around ch
            else if (positionWindow(ch, out, fUnicodeMode) )
            {
                fUnicodeMode = false;
            }
            // If all else fails, start a Unicode run
            else
            {
                iSCU = iOut;
                out[iOut++] = SCU;
                continue;
            }
        }

        return iOut - iStartOut;
    }

    public byte[] compress(String inStr)
        throws IllegalInputException, EndOfInputException
    {
        // Running out of room for output can cause non-optimal
        // compression. In order to not slow down compression too
        // much, not all intermediate state is constantly saved.

        byte [] out = new byte[inStr.length() * 2];
        char [] in = inStr.toCharArray();
        //DEBUG
        Debug.out("compress input: ",in);
        reset();
        while(true)
        {
            try
            {
                simpleCompress(in, charsRead(), out, bytesWritten());
                // if we get here things went fine.
                break;
            }
            catch (EndOfOutputException e)
            {
                // create a larger output buffer and continue
                byte [] largerOut = new byte[out.length * 2];
                System.arraycopy(out, 0, largerOut, 0, out.length);
                out = largerOut;
            }
        }
        byte [] trimmedOut = new byte[bytesWritten()];
        System.arraycopy(out, 0, trimmedOut, 0, trimmedOut.length);
        out = trimmedOut;

        Debug.out("compress output: ", out);
        return out;
    }

    /** reset is only needed to bail out after an exception and
        restart with new input */
    @Override
    public void reset()
    {
        super.reset();
        fUnicodeMode = false;
        iSCU = - 1;
    }

    /** returns the number of bytes written **/
    public int bytesWritten()
    {
        return iOut;
    }

    /** returns the number of bytes written **/
    public int charsRead()
    {
        return iIn;
    }

}
