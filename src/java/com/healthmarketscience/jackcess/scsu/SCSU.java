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
    Encoding text data in Unicode often requires more storage than using
    an existing 8-bit character set and limited to the subset of characters
    actually found in the text. The Unicode Compression Algorithm reduces
    the necessary storage while retaining the universality of Unicode.
    A full description of the algorithm can be found in document
    http://www.unicode.org/unicode/reports/tr6.html

    Summary

    The goal of the Unicode Compression Algorithm is the abilty to
    * Express all code points in Unicode
    * Approximate storage size for traditional character sets
    * Work well for short strings
    * Provide transparency for Latin-1 data
    * Support very simple decoders
    * Support simple as well as sophisticated encoders

    If needed, further compression can be achieved by layering standard
    file or disk-block based compression algorithms on top.

    <H2>Features</H2>

    Languages using small alphabets would contain runs of characters that
    are coded close together in Unicode. These runs are interrupted only
    by punctuation characters, which are themselves coded in proximity to
    each other in Unicode (usually in the ASCII range).

    Two basic mechanisms in the compression algorithm account for these two
    cases, sliding windows and static windows. A window is an area of 128
    consecutive characters in Unicode. In the compressed data stream, each
    character from a sliding window would be represented as a byte between
    0x80 and 0xFF, while a byte from 0x20 to 0x7F (as well as CR, LF, and
    TAB) would always mean an ASCII character (or control).

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
public abstract class SCSU
{
    /** Single Byte mode command values */

    /** SQ<i>n</i> Quote from Window . <p>
    If the following byte is less than 0x80, quote from
    static window <i>n</i>, else quote from dynamic window <i>n</i>.
    */

    static final byte SQ0 = 0x01; // Quote from window pair 0
    static final byte SQ1 = 0x02; // Quote from window pair 1
    static final byte SQ2 = 0x03; // Quote from window pair 2
    static final byte SQ3 = 0x04; // Quote from window pair 3
    static final byte SQ4 = 0x05; // Quote from window pair 4
    static final byte SQ5 = 0x06; // Quote from window pair 5
    static final byte SQ6 = 0x07; // Quote from window pair 6
    static final byte SQ7 = 0x08; // Quote from window pair 7

    static final byte SDX = 0x0B; // Define a window as extended
    static final byte Srs = 0x0C; // reserved

    static final byte SQU = 0x0E; // Quote a single Unicode character
    static final byte SCU = 0x0F; // Change to Unicode mode

    /** SC<i>n</i> Change to Window <i>n</i>. <p>
    If the following bytes are less than 0x80, interpret them
    as command bytes or pass them through, else add the offset
    for dynamic window <i>n</i>. */
    static final byte SC0 = 0x10; // Select window 0
    static final byte SC1 = 0x11; // Select window 1
    static final byte SC2 = 0x12; // Select window 2
    static final byte SC3 = 0x13; // Select window 3
    static final byte SC4 = 0x14; // Select window 4
    static final byte SC5 = 0x15; // Select window 5
    static final byte SC6 = 0x16; // Select window 6
    static final byte SC7 = 0x17; // Select window 7
    static final byte SD0 = 0x18; // Define and select window 0
    static final byte SD1 = 0x19; // Define and select window 1
    static final byte SD2 = 0x1A; // Define and select window 2
    static final byte SD3 = 0x1B; // Define and select window 3
    static final byte SD4 = 0x1C; // Define and select window 4
    static final byte SD5 = 0x1D; // Define and select window 5
    static final byte SD6 = 0x1E; // Define and select window 6
    static final byte SD7 = 0x1F; // Define and select window 7

    static final byte UC0 = (byte) 0xE0; // Select window 0
    static final byte UC1 = (byte) 0xE1; // Select window 1
    static final byte UC2 = (byte) 0xE2; // Select window 2
    static final byte UC3 = (byte) 0xE3; // Select window 3
    static final byte UC4 = (byte) 0xE4; // Select window 4
    static final byte UC5 = (byte) 0xE5; // Select window 5
    static final byte UC6 = (byte) 0xE6; // Select window 6
    static final byte UC7 = (byte) 0xE7; // Select window 7
    static final byte UD0 = (byte) 0xE8; // Define and select window 0
    static final byte UD1 = (byte) 0xE9; // Define and select window 1
    static final byte UD2 = (byte) 0xEA; // Define and select window 2
    static final byte UD3 = (byte) 0xEB; // Define and select window 3
    static final byte UD4 = (byte) 0xEC; // Define and select window 4
    static final byte UD5 = (byte) 0xED; // Define and select window 5
    static final byte UD6 = (byte) 0xEE; // Define and select window 6
    static final byte UD7 = (byte) 0xEF; // Define and select window 7

    static final byte UQU = (byte) 0xF0; // Quote a single Unicode character
    static final byte UDX = (byte) 0xF1; // Define a Window as extended
    static final byte Urs = (byte) 0xF2; // reserved

    /** constant offsets for the 8 static windows */
    static final int staticOffset[] =
    {
        0x0000, // ASCII for quoted tags
        0x0080, // Latin - 1 Supplement (for access to punctuation)
        0x0100, // Latin Extended-A
        0x0300, // Combining Diacritical Marks
        0x2000, // General Punctuation
        0x2080, // Currency Symbols
        0x2100, // Letterlike Symbols and Number Forms
        0x3000  // CJK Symbols and punctuation
    };

    /** initial offsets for the 8 dynamic (sliding) windows */
    static final int initialDynamicOffset[] =
    {
        0x0080, // Latin-1
        0x00C0, // Latin Extended A   //@005 fixed from 0x0100
        0x0400, // Cyrillic
        0x0600, // Arabic
        0x0900, // Devanagari
        0x3040, // Hiragana
        0x30A0, // Katakana
        0xFF00  // Fullwidth ASCII
    };

    /** dynamic window offsets, intitialize to default values. */
    int dynamicOffset[] =
    {
        initialDynamicOffset[0],
        initialDynamicOffset[1],
        initialDynamicOffset[2],
        initialDynamicOffset[3],
        initialDynamicOffset[4],
        initialDynamicOffset[5],
        initialDynamicOffset[6],
        initialDynamicOffset[7]
    };

    // The following method is common to encoder and decoder

    private int iWindow = 0;    // current active window

    /** select the active dynamic window **/
    protected void selectWindow(int iWindow)
    {
        this.iWindow = iWindow;
    }

    /** select the active dynamic window **/
    protected int getCurrentWindow()
    {
        return this.iWindow;
    }

    /**
       These values are used in defineWindow
     **/

    /**
     * Unicode code points from 3400 to E000 are not adressible by
     * dynamic window, since in these areas no short run alphabets are
     * found. Therefore add gapOffset to all values from gapThreshold */
    static final int gapThreshold = 0x68;
    static final int gapOffset = 0xAC00;

    /* values between reservedStart and fixedThreshold are reserved */
    static final int reservedStart = 0xA8;

    /* use table of predefined fixed offsets for values from fixedThreshold */
    static final int fixedThreshold = 0xF9;

    /** Table of fixed predefined Offsets, and byte values that index into  **/
    static final int fixedOffset[] =
    {
        /* 0xF9 */ 0x00C0, // Latin-1 Letters + half of Latin Extended A
        /* 0xFA */ 0x0250, // IPA extensions
        /* 0xFB */ 0x0370, // Greek
        /* 0xFC */ 0x0530, // Armenian
        /* 0xFD */ 0x3040, // Hiragana
        /* 0xFE */ 0x30A0, // Katakana
        /* 0xFF */ 0xFF60  // Halfwidth Katakana
    };

    /** whether a character is compressible */
    public static boolean isCompressible(char ch)
    {
        return (ch < 0x3400 || ch >= 0xE000);
    }

    /** reset is only needed to bail out after an exception and
        restart with new input */
    public void reset()
    {

        // reset the dynamic windows
        for (int i = 0; i < dynamicOffset.length; i++)
        {
            dynamicOffset[i] = initialDynamicOffset[i];
        }
        this.iWindow = 0;
    }
}
