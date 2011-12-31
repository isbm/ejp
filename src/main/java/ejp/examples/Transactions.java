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
import ejp.TransactionManager;
import java.util.ArrayList;

public class Transactions 
  {
    /**
   * ejp.DatabaseManager methods are all thread safe, and the saved object 
   * (and its associations) are wrapped in a single transaction.
   */
    static void simpleEjpDatabaseManagerWay(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = new Customer("jsmith1", "", "John", "Smith", "ABCProducts", "jsmith@abcproducts.com");

        customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
        customer.getOrders().add(new Order("Candle Holder", 2, 10.95, "new"));
        customer.getSupport().add(new Support("request", "new", "123-123-1234", "jsmith@abcproducts.com", "Please send ASAP"));

        dbm.saveObject(customer);
      }
    
    /**
     * If you have needs beyond saving an object and its associations, 
     * then ejp.TransactionManager is the ejp.DatabaseManager way to do it.
     * 
     * Be sure to use the ejp.TransactionManagers methods saveObject, 
     * commit, setSavepoint, rollback, etc.
     */
    static void ejpDatabaseManagerWay(DatabaseManager dbm) throws DatabaseException
      {
        new TransactionManager(dbm)
          {
            public void run() throws DatabaseException 
              {
                Customer customer = new Customer("jsmith2", "", "John", "Smith", "ABCProducts", "jsmith@abcproducts.com");

                customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
                customer.getOrders().add(new Order("Candle Holder", 2, 10.95, "new"));
                customer.getSupport().add(new Support("request", "new", "123-123-1234", "jsmith@abcproducts.com", "Please send ASAP"));
                
                saveObject(customer);

                customer = new Customer("jjacobs1", "", "John", "Jacobs", "ABCProducts", "jjacobs@abcproducts.com");
                customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
                
                saveObject(customer);
              }
          }.executeTransaction();
      }
  
    /**
     * ejp.Database's methods are also automatically wrapped in transactions.  However, 
     * you can use beginTransaction(), commit(), rollback(), setSavepoint(), and 
     * endTransaction() to override the default behavior and control the transaction 
     * (and the number of saves handled by that transaction) yourself.
     */
    static void ejpDatabaseWay(DatabaseManager dbm) throws DatabaseException
      {
        Database db = dbm.getDatabase();
        
        try
          {
            db.beginTransaction();

            Customer customer = new Customer("jpeters", "", "Joe", "Peters", "ABCProducts", "jpeters@abcproducts.com");
            customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
            customer.getOrders().add(new Order("Candle Holder", 2, 10.95, "new"));

            db.saveObject(customer);
            
            customer = new Customer("jjacobs2", "", "John", "Jacobs", "ABCProducts", "jjacobs@abcproducts.com");
            customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
            
            db.saveObject(customer);
            
            db.endTransaction();  // also commits
          }
        finally
          {
            db.close();
          }
      }
    
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);
        
        simpleEjpDatabaseManagerWay(dbm);
        ejpDatabaseManagerWay(dbm);
        ejpDatabaseWay(dbm);
        
        for (Customer customer : dbm.loadObjects(new ArrayList<Customer>(), Customer.class))
          System.out.println(customer);
      }
  }
