/**
 * Copyright (C) 2006 - present David Bulmore  
 * All Rights Reserved.
 *
 * This file is part of Easy Java Persistence.
 *
 * EJP is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the accompanying license 
 * for more details.
 *
 * You should have received a copy of the license along with EJP; if not, 
 * go to http://www.EasierJava.com and download the latest version.
 */

package ejp.interfaces;

import java.io.Reader;

/**
 * This interface should be used in place of Reader in your objects, 
 * and is used to handle Reader for PreparedStatement.[get/set]CharacterStream.
 */

public class CharacterStreamAdapter implements CharacterStream
  {
    int length;
    Reader reader;
    
    public CharacterStreamAdapter(int length, Reader reader)
      {
        this.length = length;
        this.reader = reader;
      }
    
    /**
     * Returns the length of data in the stream.  When retrieved from the database the length is unknown, -1 will be returned.
     * @return the length of data in the stream.
     */
  
    public int getLength() { return length; }
    
    /**
     * Return the InputStream to read from.
     * @return the InputStream to read from
     */
    
    public Reader getReader() { return reader; }
  }
