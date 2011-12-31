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

package ejp.utilities;

public class XMLParserException extends CommonException
  {
    private static final long serialVersionUID = 100L;
    
    public XMLParserException(String message) { super(message); }

    public XMLParserException(Throwable cause) throws XMLParserException
      {
        super(cause);

        if (cause instanceof XMLParserException)
          throw (XMLParserException)cause;
      }

    public XMLParserException(String message, Throwable cause) throws XMLParserException
      {
        super(message, cause);

        if (cause instanceof XMLParserException)
          throw (XMLParserException)cause;
      }
  }

