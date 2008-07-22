package com.healthmarketscience.jackcess.scsu;

import java.io.*;
import java.util.*;

/**
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
	Class CompressMain

	A small commandline driver interface for the compression routines
	Use the /? to get usage
*/
public class CompressMain
{
	static void usage()
	{
		System.err.println("java CompressMain /?               : this usage information\n");
		System.err.println("java CompressMain /random    	   : random test\n");
		System.err.println("java CompressMain /suite           : suite test\n");
		System.err.println("java CompressMain /suite <file>    : file test (file data may include \\uXXXX)\n");
		System.err.println("java CompressMain <string>    	   : string test (string may include \\uXXXX)\n");
		System.err.println("java CompressMain /roundtrip <file>: check Unicode file for roundtrip\n");
		System.err.println("java CompressMain /compress <file> : compresses Unicode files (no \\uXXXX)\n");
		System.err.println("java CompressMain /expand <file>   : expands into Unicode files\n");
		System.err.println("java CompressMain /byteswap <files>: swaps byte order of Unicode files\n");
		System.err.println("java CompressMain /display <files> : like expand, but creates a dump instead\n");
		System.err.println("java CompressMain /parse <files>   : parses \\uXXXX into binary Unicode\n");
	}

    static void analyze(String text, int inlength, String result, int outlength)
    {
        boolean fSuccess = text.equals(result);
        Debug.out(fSuccess ? "Round trip OK" : "Round trip FAILED");
        if (!fSuccess && result != null)
        {
            int iLim = Math.min(text.length(), result.length());
            for (int i = 0; i < iLim; i++)
            {
                if (text.charAt(i) != result.charAt(i))
                {
                    Debug.out("First Mismatch at  "+ i +"=", result.charAt(i) );
                    Debug.out("Original character "+ i +"=", text.charAt(i) );
                    break;
                }
            }
        }
        else
        {
            Debug.out("Compressed: "+inlength+" chars to "+outlength+" bytes.");
            Debug.out(" Ratio: "+(outlength == 0 ? 0 :(outlength * 50 / inlength))+"%.");
        }
    }

    static void test2(String text)
    {
        byte bytes[] = null;
        String result = null;
        Debug.out("SCSU:\n");
        Compress compressor = new Compress();
        try
        {
            bytes = compressor.compress(text);
            Expand display = new Expand();
            result = display.expand(bytes);
            Debug.out("Input:  ", text.toCharArray());
            Debug.out("Result: ", result.toCharArray());
            Debug.out("");
            Expand expander = new Expand();
            result = expander.expand(bytes);
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
        int inlength = compressor.charsRead();
        int outlength = compressor.bytesWritten();
        analyze(text, inlength, result, outlength);
    }

    static void test(String text) throws Exception
    {
      test(text, false);
    }

  static void test(String text, boolean shouldFail)
      throws Exception
    {
        // Create an instance of the compressor
        Compress compressor = new Compress();

        byte [] bytes = null;
        String result = null;
        Exception failure = null;
        try {
            // perform compression
            bytes = compressor.compress(text);
        }
        catch(Exception e)
        {
            failure = e;
        }

        if(shouldFail) {
          if(failure == null) {
            throw new RuntimeException("Did not fail");
          }
          return;
        }

        if(failure != null) {
          throw failure;
        }

        Expand expander = new Expand();
        // perform expansion
        result = expander.expand(bytes);

        // analyze the results
        int inlength = compressor.charsRead();
        int outlength = compressor.bytesWritten();
        analyze(text, inlength, result, outlength);

    }

    public static void display(byte [] input)
    {
        try
        {
            Expand expand = new Expand();
            String text = expand.expand(input);
            Debug.out(text.toCharArray());
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
    }

    public static String parse(String input)
    {
        StringTokenizer st = new StringTokenizer(input, "\\", true);
        Debug.out("Input: ", input);

        StringBuffer sb = new StringBuffer();

        while(st.hasMoreTokens())
        {
            String token = st.nextToken();
                    Debug.out("Token: ", token);
            if (token.charAt(0) == '\\' && token.length() == 1)
            {
                if(st.hasMoreTokens())
                {
                    token = st.nextToken();
                }
                if(token.charAt(0) == 'u')
                {
                    Debug.out("Token: "+ token+ " ", sb.toString());
                    String hexnum;
                    if (token.length() > 5)
                    {
                        hexnum = token.substring(1,5);
                        token = token.substring(5);
                    }
                    else
                    {
                        hexnum = token.substring(1);
                        token = "";
                    }
                    sb.append((char)Integer.parseInt(hexnum, 16));
                }
            }
            sb.append(token);
        }
        return sb.toString();
    }

    public static void randomTest(int nTest)
      throws Exception
    {
        Random random = new Random();

        for(int n=0; n < nTest; n++)
        {
            int iLen = (int) (20 * random.nextFloat());
            StringBuffer sb = new StringBuffer(iLen);

            for(int i = 0; i < iLen; i++)
            {
                sb.append((char) (0xFFFF * random.nextFloat()));
            }

            test(sb.toString());
        }
    }

    @SuppressWarnings("deprecation")
    public static void fileTest(String name)
        throws Exception
    {
        DataInputStream dis = new DataInputStream(new FileInputStream(name));

        int iLine = 0;

        while(dis.available() != 0)
        {
            String line = dis.readLine();
            Debug.out("Line "+ iLine++ +" "+line);
            test(parse(line), false ); //false);// initially no debug info
        }
    }

    public static void displayFile(String name)
            throws IOException
    {
        DataInputStream dis = new DataInputStream(new FileInputStream(name));

        byte bytes[] = new byte[dis.available()];
        dis.read(bytes);
        display(bytes);
    }

    public static void decodeTest(String name)
           throws IOException
    {
        DataInputStream dis = new DataInputStream(new FileInputStream(name));

        byte bytes[] = new byte[dis.available()];
        dis.read(bytes);

        Expand expand = new Expand();

        char [] chars = null;
        try
        {
            String text = expand.expand(bytes);
            chars = text.toCharArray();
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
        int inlength = expand.bytesRead();
        int iDot = name.lastIndexOf('.');
        StringBuffer sb = new StringBuffer(name);
        sb.setLength(iDot + 1);
        sb.append("txt");
        String outName = sb.toString();

        int outlength = expand.charsWritten();

        Debug.out("Expanded "+name+": "+inlength+" bytes to "+outName+" " +outlength+" chars." + " Ratio: "+(outlength == 0 ? 0 :(outlength * 200 / inlength))+"%.");

        if (chars == null)
            return;

        writeUnicodeFile(outName, chars);
    }

    /** most of the next 3 functions should not be needed by JDK11 and later */
    private static int iMSB = 1;

    public static String readUnicodeFile(String name)
    {
        try
        {
            FileInputStream dis = new FileInputStream(name);

            byte b[] = new byte[2];
            StringBuffer sb = new StringBuffer();
            char ch = 0;

            iMSB = 1;
            int i = 0;
            for(i = 0; (dis.available() != 0); i++)
            {
                b[i%2] = (byte) dis.read();

                if ((i & 1) == 1)
                {
                    ch = Expand.charFromTwoBytes(b[(i + iMSB)%2], b[(i + iMSB + 1) % 2]);
                }
                else
                {
                    continue;
                }
                if (i == 1 && ch == '\uFEFF')
                    continue; // throw away byte order mark

                if (i == 1 && ch == '\uFFFE')
                {
                    iMSB ++;  // flip byte order
                    continue; // throw away byte order mark
                }
                sb.append(ch);
             }

            return sb.toString();
        }
        catch (IOException e)
        {
            System.err.println(e);
            return "";
        }
    }

    public static void writeUnicodeFile(String outName, char [] chars)
            throws IOException
    {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(outName));
        if ((iMSB & 1) == 1)
        {
            dos.writeByte(0xFF);
            dos.writeByte(0xFE);
        }
        else
        {
            dos.writeByte(0xFE);
            dos.writeByte(0xFF);
        }
        byte b[] = new byte[2];
        for (int ich = 0; ich < chars.length; ich++)
        {
            b[(iMSB + 0)%2] = (byte) (chars[ich] >>> 8);
            b[(iMSB + 1)%2] = (byte) (chars[ich] & 0xFF);
            dos.write(b, 0, 2);
        }
    }

    static void byteswap(String name)
        throws IOException
    {
        String text = readUnicodeFile(name);
        char chars[] = text.toCharArray();
        writeUnicodeFile(name, chars);
    }

    @SuppressWarnings("deprecation")
    public static void parseFile(String name)
        throws IOException
    {
        DataInputStream dis = new DataInputStream(new FileInputStream(name));

        byte bytes[] = new byte[dis.available()];
        dis.read(bytes);

        // simplistic test
        int bom = (char) bytes[0] + (char) bytes[1];
        if (bom == 131069)
        {
            // FEFF or FFFE detected (either one sums to 131069)
            Debug.out(name + " is already in Unicode!");
            return;
        }

        // definitely assumes an ASCII file at this point
        String text = new String(bytes, 0);

        char chars[] = parse(text).toCharArray();
        writeUnicodeFile(name, chars);
        return;
    }

    public static void encodeTest(String name)
        throws Exception
    {
        String text = readUnicodeFile(name);

        // Create an instance of the compressor
        Compress compressor = new Compress();

        byte [] bytes = null;

        // perform compression
        bytes = compressor.compress(text);

        int inlength = compressor.charsRead();
        int iDot = name.lastIndexOf('.');
        StringBuffer sb = new StringBuffer(name);
        sb.setLength(iDot + 1);
        sb.append("csu");
        String outName = sb.toString();

        DataOutputStream dos = new DataOutputStream(new FileOutputStream(outName));
        dos.write(bytes, 0, bytes.length);

        int outlength = compressor.bytesWritten();

        Debug.out("Compressed "+name+": "+inlength+" chars to "+outName+" " +outlength+" bytes." + " Ratio: "+(outlength == 0 ? 0 :(outlength * 50 / inlength))+"%.");
    }

    public static void roundtripTest(String name)
      throws Exception
    {
      test(readUnicodeFile(name), false);// no debug info
    }

    /** The Main function */
    public static void main(String args[])
      throws Exception
    {
        int iArg = args.length;

        try
        {
            if (iArg != 0)
            {
                if (args[0].equalsIgnoreCase("/compress"))
                {
                    while (--iArg > 0)
                    {
                        encodeTest(args[args.length - iArg]);
                    }
                }
                else if (args[0].equalsIgnoreCase("/parse"))
                {
                    while (--iArg > 0)
                    {
                        parseFile(args[args.length - iArg]);
                    }
                }
                else if (args[0].equalsIgnoreCase("/expand"))
                {
                    while (--iArg > 0)
                    {
                        decodeTest(args[args.length - iArg]);
                    }
                }
                else if (args[0].equalsIgnoreCase("/display"))
                {
                    while (--iArg > 0)
                    {
                        displayFile(args[args.length - iArg]);
                    }
                }
                else if (args[0].equalsIgnoreCase("/roundtrip"))
                {
                    while (--iArg > 0)
                    {
                        roundtripTest(args[args.length - iArg]);
                    }
                }
                else if (args[0].equalsIgnoreCase("/byteswap"))
                {
                    while (--iArg > 0)
                    {
                        byteswap(args[args.length - iArg]);
                    }
                }else if (args[0].equalsIgnoreCase("/random"))
                {
                    randomTest(8);
                }
                else if (args[0].equalsIgnoreCase("/suite"))
                {
                    if (iArg == 1)
                    {
                        suiteTest();
                    }
                    else
                    {
                        while (--iArg > 0)
                        {
                            fileTest(args[args.length - iArg]);
                        }
                    }
                }
    			else if (args[0].equalsIgnoreCase("/?"))
    			{
    				usage();
    			}
                else
                {
                    while (iArg > 0)
                    {
                        test2(parse(args[--iArg]));
                    }
                }
            }
            else
            {
                usage();
            }
        }
        catch (IOException e)
        {
            System.err.println(e);
        }
        try
        {
            System.err.println("Done. Press enter to exit");
            System.in.read();
        }
        catch (IOException e)
        {

        }
    }

    static void suiteTest()
      throws Exception
    {
        Debug.out("Standard Compression test suite:");
        test("Hello \u9292 \u9192 World!");
        test("Hell\u0429o \u9292 \u9192 W\u00e4rld!");
        test("Hell\u0429o \u9292 \u9292W\u00e4rld!");

        test("\u0648\u06c8"); // catch missing reset
        test("\u0648\u06c8");

        test("\u4444\uE001"); // lowest quotable
        test("\u4444\uf2FF"); // highest quotable
        test("\u4444\uf188\u4444");
        test("\u4444\uf188\uf288");
        test("\u4444\uf188abc\0429\uf288");
        test("\u9292\u2222");
        test("Hell\u0429\u04230o \u9292 \u9292W\u00e4\u0192rld!");
        test("Hell\u0429o \u9292 \u9292W\u00e4rld!");
        test("Hello World!123456");
        test("Hello W\u0081\u011f\u0082!"); // Latin 1 run

        test("abc\u0301\u0302");  // uses SQn for u301 u302
        test("abc\u4411d");      // uses SQU
        test("abc\u4411\u4412d");// uses SCU
        test("abc\u0401\u0402\u047f\u00a5\u0405"); // uses SQn for ua5
        test("\u9191\u9191\u3041\u9191\u3041\u3041\u3000"); // SJIS like data
        test("\u9292\u2222");
        test("\u9191\u9191\u3041\u9191\u3041\u3041\u3000");
        test("\u9999\u3051\u300c\u9999\u9999\u3060\u9999\u3065\u3065\u3065\u300c");
        test("\u3000\u266a\u30ea\u30f3\u30b4\u53ef\u611b\u3044\u3084\u53ef\u611b\u3044\u3084\u30ea\u30f3\u30b4\u3002");

        test(""); // empty input
        test("\u0000"); // smallest BMP character
        test("\uFFFF"); // largest BMP character

        test("\ud800\udc00"); // smallest surrogate
        test("\ud8ff\udcff"); // largest surrogate pair


        Debug.out("\nTHESE TESTS ARE SUPPOSED TO FAIL:");
        test("\ud800 \udc00", true); // unpaired surrogate (1)
        test("\udc00", true); // unpaired surrogate (2)
        test("\ud800", true); // unpaired surrogate (3)
   }
}
