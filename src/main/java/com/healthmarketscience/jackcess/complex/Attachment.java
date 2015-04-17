/*
Copyright (c) 2011 James Ahlborn

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

package com.healthmarketscience.jackcess.complex;

import java.io.IOException;
import java.util.Date;

/**
 * Complex value corresponding to an attachment.
 *
 * @author James Ahlborn
 */
public interface Attachment extends ComplexValue 
{
  public byte[] getFileData() throws IOException;

  public void setFileData(byte[] data);

  public byte[] getEncodedFileData() throws IOException;

  public void setEncodedFileData(byte[] data);

  public String getFileName();

  public void setFileName(String fileName);
  
  public String getFileUrl();

  public void setFileUrl(String fileUrl);
  
  public String getFileType();

  public void setFileType(String fileType);
  
  public Date getFileTimeStamp();

  public void setFileTimeStamp(Date fileTimeStamp);
  
  public Integer getFileFlags();

  public void setFileFlags(Integer fileFlags);  
}
