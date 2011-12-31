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

import java.lang.reflect.InvocationTargetException;

public class CommonException extends Exception
  {
    private static final long serialVersionUID = 100L;

    public CommonException(String message) { super(message); }
    public CommonException(String message, Throwable cause) { super(message, cause); }

    public CommonException(Throwable cause)
      {
        super(cause);

        if (cause instanceof RuntimeException)
          throw (RuntimeException)cause;
      }

    public static Throwable getNormalizedCause(Throwable t)
      {
        while (t.getCause() != null)
          t = t.getCause();

        return t;
      }
    
    public static String getNormalizedMessage(Throwable t)
      {
        if (t instanceof IllegalAccessException)
          return "IllegalAccessException: Can't access member due to visibility (non-public)";
        else if (t instanceof InvocationTargetException)
          return getNormalizedCause(t).toString();
        else if (t instanceof InstantiationException)
          return "InstantiationException: Can't create new instance because of visibility (not public), or there's no suitable\n" +
                  "constructor (might need an empty constructor), or it's an interface, or it's an abstract class";

        return t.toString();
      }
  }
