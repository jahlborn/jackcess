// Copyright (c) 2004 Health Market Science, Inc.

package com.healthmarketscience.jackcess;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.healthmarketscience.jackcess.Database;

import java.io.File;
import junit.framework.TestCase;

/** 
 *  @author Rob Di Marco
 */ 
public class ImportTest extends TestCase
{

  /** The logger to use. */
  private static final Log LOG = LogFactory.getLog(ImportTest.class);
  public ImportTest(String name)
  {
    super(name);
  }

  private Database create() throws Exception {
    File tmp = File.createTempFile("databaseTest", ".mdb");
    tmp.deleteOnExit();
    return Database.create(tmp);
  }

  public void testImportFromFile() throws Exception
  {
    Database db = create();
    db.importFile("test", new File("test/data/sample-input.tab"), "\\t");
  }

  public void testImportFromFileWithOnlyHeaders() throws Exception
  {
    Database db = create();
    db.importFile("test", new File("test/data/sample-input-only-headers.tab"),
                  "\\t");
  }

} 
