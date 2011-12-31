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
import ejp.Database.InParameter;
import ejp.Database.OutParameter;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.Result;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

@SuppressWarnings("unchecked")
public class EjpDatabaseExamples 
  {
    /*
     * Saves are the same for both ejpDatabase and ejp.DatabaseManager and 
     * can be done with saveObject, executeUpdate, and parameterizedUpdate.
     */
    static void saves(Database db) throws DatabaseException
      {
        db.parameterizedUpdate("insert into customers (customer_id, password, first_name, last_name, company_name, email) values(?,?,?,?,?,?)", 
                                "jjohnson", "mypasswd5", "John", "Johnson", "ABCProducts", "jjohnson@abcproducts.com");
        
        db.parameterizedUpdate("insert into orders (customer_id, product, quantity, price, status) values(?,?,?,?,?)", 
                                "jjohnson", "NFL Broncos Coffee Cup", 100, 1.00, "unverified");

        Customer customer = new Customer("deisenhower", "mypasswd5", "Dwight", "Eisenhower", "United States", "deisenhower@unitedstates.gov");

        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Can I have my bust on a dollar, please."));
        customer.getSupport().add(new Support("Response", "Pending", "no phone", "deisenhower@unitedstates.gov", "Yes, but you may have to share it."));
        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Share it with who?"));

        customer.getOrders().add(new Order("Dwight D. Eisenhower Dollar", new Integer(100), new Double(1.00), "unverified"));
        customer.getOrders().add(new Order("Susan B. Anthony Dollar", new Integer(10), new Double(1.52), "unverified"));

        db.saveObject(customer);
      }
    
    /*
     * Transactions in ejp.Database are handled with beginTransaction, commit, 
     * rollback, setSavepoint, endTransaction.  endTransaction also commits.
     */
    static void transactions(Database db) throws DatabaseException
      {
        Customer customer;

        db.beginTransaction();
        
        customer = new Customer("ssmith", "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
        customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));
        
        db.saveObject(customer);

        customer = new Customer("ssmith2", "mypasswd", "Scott", "Smith2", "ABCProducts", "ssmith2@abcproducts.com");
        customer.getOrders().add(new Order("NFL Denver Broncos Team T-Shirts", 150, 12.00, "unverified"));
        
        db.saveObject(customer);

        db.endTransaction();
      }
    
    /*
     * Batch updates are handled with beginBatch, executeBatch, and endBatch.  
     * Saves and updates can be made with any of the save/update methods 
     * (saveObject, executeUpdate, and parameterizedUpdate).
     */
    static void batchUpdates(Database db) throws DatabaseException
      {
        Customer customer;

        db.beginBatch();
        
        for (int i = 0; i < 5; i++)
          {
            customer = new Customer("ssmith_" + i, "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
  
            for (int i2 = 0; i2 < 5; i2++)
              customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts" + i2, 150, 12.00, "unverified"));
            
            db.saveObject(customer);
          }
        
        db.executeBatch();
        db.endBatch();
      }
    
    /*
     * ejp.Database queries are done with queryObject, execute, executeQuery, 
     * parameterizedQuery, or storedProcedure.  all ejp.Database queries return
     * an ejp.Result, which is a database cursor and can be used to traverse
     * the rows returned by the query.  ejp.Result is a mapper and can always 
     * be used to load data into one or more of your objects.
     */
    static void queries(Database db) throws DatabaseException
      {
        Result<Customer> result = db.queryObject(Customer.class, "where customer_id = ?", "ssmith");
        
        if (result.hasNext())
          System.out.println(result.next());
        
        result = db.queryObject(Customer.class);
        
        for (Customer c : result)
          System.out.println(c);
        
        result = db.parameterizedQuery(Customer.class, "select * from customers where customer_id = ?", "jjohnson");

        for (Customer c : result)
          System.out.println(c);
      }
    
    /*
     * Stored procedures are called via JDBC's portable stored procedure 
     * syntax (i.e. {?= call stored_function_or_procedure(?,?,[...])} ).  
     * Supplying the parameters is done with InParameter, OutParameter, 
     * and InOutParameter.  storedProcedure returns a Result that can be
     * composed of multiple result sets and update counts.  You can use
     * results getMoreResults(), isUpdateCount(), getUpdateCount(). When
     * done processing results, you can get the statement to access the
     * out parameters.
     */
    static void storedProcedure(Database db) throws DatabaseException, SQLException
      {
        Result result = db.storedProcedure("{call getCustomerOrdersAndSupport(?,?)}", 
                                           new InParameter(1, "deisenhower"),
                                           new OutParameter(2, Types.DOUBLE, 2));

        // Map orders
        for (Order order : (Result<Order>)result.setClass(Order.class))
          {
            order.setStatus("verified");
            db.saveObject(order);

            System.out.println(order);
          }

        result.getMoreResults();

        // Map support
        for (Support s : (Result<Support>)result.setClass(Support.class))
          System.out.println(s);

        System.out.println("Order total: " + ((CallableStatement)result.getStatement()).getDouble(2));
      }
    
    public static void main(String[] args) throws DatabaseException, SQLException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);

        dbm.executeUpdate("delete from customers");
        
        Database db = dbm.getDatabase();
        
        try
          {
            saves(db);
            transactions(db);
            batchUpdates(db);
            queries(db);
            //storedProcedure(db); MySql Only
          } 
        finally 
          {
            db.close(); 
          }
      }
  }
