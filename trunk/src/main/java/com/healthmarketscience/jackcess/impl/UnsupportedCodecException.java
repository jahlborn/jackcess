/*
Copyright (c) 2012 James Ahlborn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess.impl;

/**
 * Exception thrown by a CodecHandler to indicate that the current encoding is
 * not supported.  This generally indicates that a different CodecProvider
 * needs to be chosen.
 *
 * @author James Ahlborn
 */
public class UnsupportedCodecException extends UnsupportedOperationException 
{
  private static final long serialVersionUID = 20120313L;  

  public UnsupportedCodecException(String msg) 
  {
    super(msg);
  }

  public UnsupportedCodecException(String msg, Throwable t) 
  {
    super(msg, t);
  }

  public UnsupportedCodecException(Throwable t) 
  {
    super(t);
  }
}
