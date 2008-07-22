package com.healthmarketscience.jackcess.scsu;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
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
 * A number of helpful output routines for debugging. Output can be
 * centrally enabled or disabled by calling Debug.set(true/false);
 * All methods are statics;
 */

public class Debug
{
  
    private static final Log LOG = LogFactory.getLog(Debug.class); 
  
    // debugging helper
    public static void out(char [] chars)
    {
         out(chars, 0);
    }

    public static void out(char [] chars, int iStart)
    {
        if (!LOG.isDebugEnabled()) return;
        StringBuilder msg = new StringBuilder();

        for (int i = iStart; i < chars.length; i++)
        {
            if (chars[i] >= 0 && chars[i] <= 26)
            {
                msg.append("^"+(char)(chars[i]+0x40));
            }
            else if (chars[i] <= 255)
            {
                msg.append(chars[i]);
            }
            else
            {
                msg.append("\\u"+Integer.toString(chars[i],16));
            }
        }
        LOG.debug(msg.toString());
    }

    public static void out(byte [] bytes)
    {
        out(bytes, 0);
    }
    public static void out(byte [] bytes, int iStart)
    {
        if (!LOG.isDebugEnabled()) return;
        StringBuilder msg = new StringBuilder();

        for (int i = iStart; i < bytes.length; i++)
        {
            msg.append(bytes[i]+",");
        }
        LOG.debug(msg.toString());
    }

    public static void out(String str)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(str);
    }

    public static void out(String msg, int iData)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg + iData);
    }
    public static void out(String msg, char ch)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg + "[U+"+Integer.toString(ch,16)+"]" + ch);
    }
    public static void out(String msg, byte bData)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg + bData);
    }
    public static void out(String msg, String str)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg + str);
    }
    public static void out(String msg, char [] data)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg);
        out(data);
    }
    public static void out(String msg, byte [] data)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg);
        out(data);
    }
    public static void out(String msg, char [] data, int iStart)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg +"("+iStart+"): ");
        out(data, iStart);
    }
    public static void out(String msg, byte [] data, int iStart)
    {
        if (!LOG.isDebugEnabled()) return;

        LOG.debug(msg+"("+iStart+"): ");
        out(data, iStart);
    }
}
