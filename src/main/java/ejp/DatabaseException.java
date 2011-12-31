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

package ejp;

import ejp.utilities.CommonException;

public class DatabaseException extends CommonException
  {
    private static final long serialVersionUID = 100L;
    public static final String DATABASE_CLOSED = "Database is closed";
    public static final String SQL_STATEMENT_NULL = "SQL statement is null or empty";
    
    public DatabaseException(String message) { super(message); }
    public DatabaseException(String message, Throwable cause) { super(message, cause); }

    public DatabaseException(Throwable cause) throws DatabaseException 
      {
        super(cause);

        if (cause instanceof DatabaseException)
          throw (DatabaseException)cause;
      }
  }

