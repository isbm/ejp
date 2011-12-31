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

import java.util.List;
import ejp.Database;

/**
 * This interface is optionally implemented to provide overriding of
 * auto-generated keys.
 */

public interface GeneratedKeys 
  {
    /**
     * <p>This method allows for overriding ejps handling of auto-generated keys.  
     * This method should simply do whatever is required to populate 
     * auto-generated key fields.</p>
     * 
     * <p>Most databases allow you to obtain the last 
     * key with something like:</p>
     * 
     * <pre>
     *     mysql: select LAST_INSERT_ID() as id
     *     hsql: select IDENTITY() as id from table_name
     *     postgres: select currval('table_name_table_column_seq') as id
     * </pre>
     * 
     * <p>It's actually thread safe, as most database implementations of Statement 
     * simply store the last key returned value in the statement until its next use.</p>
     * 
     * <p>So, the implementation of your class might look like:</p>
     * 
     * <pre>
     * public class Customer implements GeneratedKeys
     *   {
     *     Integer customerId;
     * 
     *     public Integer getCustomerId() { return customerId; }
     *     public void setCustomerId(Integer id) { customerId = id; }
     * 
     *     boolean setGeneratedKeys(Database db, List keysRequested, List keysReturned)
     *       {
     *         result = db.executeQuery("select LAST_INSERT_ID() as id");
     * 
     *         if (result.hasNext())
     *           {
     *             setCustomerId(result.getColumnValue("id"));
     * 
     *             return true;
     *           }
     * 
     *         return false;
     *       }
     *   }
     * </pre>
     *
     * @param db the current database handler
     * @param keysRequested the List containing the keys requested (if any)
     * @param keysReturned the List containing the keys returned (if any)
     *
     * @return should return true if the keys we're handled successfully, 
     *         and false if they were not.
     */
    boolean setGeneratedKeys(Database db, List keysRequested, List keysReturned);
  }
