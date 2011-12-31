/**
 * Copyright (C) 2006 - present David Bulmore  
 * All Rights Reserved.
 *
 * This file is part of Easy Java Websites.
 *
 * EJW is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the accompanying license 
 * for more details.
 *
 * You should have received a copy of the license along with EJW; if not, 
 * go to http://www.EasierJava.com and download the latest version.
 */

package ejp.utilities;

public class LicenseException extends CommonException
  {
    private static final long serialVersionUID = 100L;
    
    public LicenseException(String message) { super(message); }
    public LicenseException(String message, Throwable cause) { super(message, cause); }

    public LicenseException(Throwable cause) throws LicenseException
      {
        super(cause);

        if (cause instanceof LicenseException)
          throw (LicenseException)cause;
      }
  }

