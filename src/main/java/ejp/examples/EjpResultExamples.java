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

package ejp.examples;

import ejp.Database;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.Result;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

public class EjpResultExamples 
  {
    /*
     * Some data to play with.
     */
    static void saveData(Database db) throws DatabaseException
      {
        Customer customer = new Customer("jjohnson", "mypasswd5", "John", "Johnson", "ABCProducts", "jjohnson@abcproducts.com");
        customer.getOrders().add(new Order("NFL Broncos Coffee Cup", 100, 1.00, "unverified"));

        db.saveObject(customer);

        customer = new Customer("deisenhower", "mypasswd5", "Dwight", "Eisenhower", "United States", "deisenhower@unitedstates.gov");

        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Can I have my bust on a dollar, please."));
        customer.getSupport().add(new Support("Response", "Pending", "no phone", "deisenhower@unitedstates.gov", "Yes, but you may have to share it."));
        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Share it with who?"));

        customer.getOrders().add(new Order("Dwight D. Eisenhower Dollar", new Integer(100), new Double(1.00), "unverified"));
        customer.getOrders().add(new Order("Susan B. Anthony Dollar", new Integer(10), new Double(1.52), "unverified"));

        db.saveObject(customer);

        customer = new Customer("ssmith", "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
        customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));
        
        db.saveObject(customer);

        customer = new Customer("ssmith2", "mypasswd", "Scott", "Smith2", "ABCProducts", "ssmith2@abcproducts.com");
        customer.getOrders().add(new Order("NFL Denver Broncos Team T-Shirts", 150, 12.00, "unverified"));
        
        db.saveObject(customer);
      }
    
    /*
     * If your result set concurrency is ResultSet.CONCUR_UPDATABLE, 
     * you can update rows in the result on the fly.
     */
    static void updateRow(Database db) throws DatabaseException
      {
        // The following two method calls depend on the database being used, they may be optional
        db.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
        db.setResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE);
        Result<Customer> result = db.queryObject(Customer.class, "where customer_id = ?", "ssmith");

        for (Customer c : result)
          {
            result.setColumnValue("company_name", "ACBProducts");
            result.updateRow();
          }
      }
    
    /*
     * Result is a ListIterator and Iterable.
     */
    static void iteratingResults(Database db) throws DatabaseException
      {
        Result<Customer> result = db.queryObject(Customer.class, "where customer_id = ?", "ssmith");
        
        for (Customer c : result)
          System.out.println(c);
        
        result = db.queryObject(Customer.class);
        
        while (result.hasNext())
          System.out.println(result.next());
        
        result = db.queryObject(Customer.class);
        
        for (Iterator it = result.iterator(); it.hasNext();)
          System.out.println(it.next());
      }
    
    /*
     * Load a collection with all the result rows.
     */
    static void collectionLoading(Database db) throws DatabaseException
      {
        Collection<Customer> customers = db.queryObject(Customer.class).loadObjects(new ArrayList<Customer>(), Customer.class);
        
        for (Customer c : customers)
          System.out.println(c);
      }
    
    /*
     * Result cursors are bidirectional.  You can use hasNext, next, 
     * hasPrevious, previous, isFirst, first, isLast, last, isBeforeFirst, 
     * beforeFirst, isAfterLast, afterLast.  You can also use Iterator and 
     * ListIterator.
     */
    static void movingBackAndForth(Database db) throws DatabaseException
      {
        Result<Order> result = db.queryObject(Order.class);

        while (result.hasNext())
          System.out.println(result.next());

        while (result.hasPrevious())
          System.out.println(result.previous());
        
        result.last();

        System.out.println();
        
        System.out.println(result.current());
        
        while (result.hasPrevious())
          System.out.println(result.previous());
        
        System.out.println();
        
        result = db.queryObject(Order.class);
        
        ListIterator it;
        
        for (it = result.listIterator(); it.hasNext();)
          System.out.println(it.next());
        
        for (; it.hasPrevious();)
          System.out.println(it.previous());
      }

    public static void main(String[] args) throws DatabaseException, SQLException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);

        dbm.executeUpdate("delete from customers");
        
        Database db = dbm.getDatabase();
        
        try
          {
            saveData(db);
            updateRow(db);
            iteratingResults(db);
            collectionLoading(db);
            movingBackAndForth(db);
          } 
        finally 
          {
            db.close(); 
          }
      }
  }
