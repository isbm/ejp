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
import java.util.ArrayList;
import java.util.Collection;

@SuppressWarnings("unchecked")
public class QueriesAndUpdates 
  {
    /**
     * Saves and updates are the same with ejp.DatabaseManager and ejp.Database.
     */
    static void savesAndUpdates(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate("delete from customers");
        
        // The old fashioned SQL way
        dbm.parameterizedUpdate("insert into customers (customer_id, password, first_name, last_name, company_name, email) values(?,?,?,?,?,?)", 
                                "jjohnson", "mypasswd5", "John", "Johnson", "ABCProducts", "jjohnson@abcproducts.com");
        
        dbm.parameterizedUpdate("insert into orders (customer_id, product, quantity, price, status) values(?,?,?,?,?)", 
                                "jjohnson", "NFL Broncos Coffee Cup", 100, 1.00, "unverified");

        // The modern object oriented way
        Customer customer = new Customer("ssmith", "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
        customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));
        
        dbm.saveObject(customer);
      }
    
    /**
     * ejp.DatabaseManager queries can be performed with loadObject(), loadObjects(), executeQuery(), 
     * and parameterizedQuery().  loadObject() returns a single object, while loadObjects(), 
     * executeQuery(), and parameterizedQuery() all return a collection.
     */
    static void ejpDatabaseManagerQueries(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = dbm.loadObject(new Customer("jjohnson"));
        System.out.println(customer);
        
        customer = dbm.loadObject(Customer.class, "where customer_id = ?", "jjohnson");
        System.out.println(customer);

        Collection<Customer> customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class);

        for (Customer c : customers)
          System.out.println(c);
        
        customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class, 
                "from customers c, orders o where c.customer_id = o.customer_id and o.status = ?", "unverified");
        
        for (Customer c : customers)
          System.out.println(c);
      }
    
    /**
     * ejp.Database is the heart of EJP.  All ejp.DatabaseManager methods 
     * simply wrap ejp.Database functionality for one-line of code ease of use.
     * 
     * The following demonstrates a query with ejp.Database.  ejp.Database 
     * queries can be performed with loadObject(), loadObjects(), queryObject(), 
     * execute(), executeQuery(), parameterizedQuery(), and preparedSatement().  
     * 
     * All queries with ejp.Database, except loadObject() and loadObjects(), return 
     * an ejp.Result object, which wraps a java.sql.ResultSet.  ejp.Result is a 
     * database cursor and this is what you want when handling large query results.  
     * Since a cursor is a database resource, you will want to close ejp.Result when 
     * you are done. Closing ejp.Database also closes its ejp.Results.
     */
    static void ejpDatabaseQueries(DatabaseManager dbm) throws DatabaseException
      {
        Database db = dbm.getDatabase();

        try
          {
            Result<Customer> customers = db.queryObject(Customer.class);
            
            for (Customer c : customers)
              System.out.println(c);
          }
        finally { db.close(); }
      }
    
    /**
     * ejp.Database and ejp.Databasemanager's executeQuery() and parameterizedQuery()
     * are the same with the exception that ejp.Database returns an ejp.Result, while
     * ejp.Databasemanager returns a collection.
     */
    static void executeQueryAndParameterizedQuery(DatabaseManager dbm) throws DatabaseException
      {
        Collection<Customer> customers = dbm.parameterizedQuery(new ArrayList<Customer>(), Customer.class, 
                "select c.* from customers c, orders o where c.customer_id = o.customer_id and o.status = ?", "unverified");

        for (Customer c : customers)
          System.out.println(c);
        
        Collection<CustomerOrder> customerOrders = dbm.parameterizedQuery(new ArrayList<CustomerOrder>(), CustomerOrder.class, 
                "select * from customers c, orders o where c.customer_id = o.customer_id and o.status = ?", "unverified");

        for (CustomerOrder co : customerOrders)
          System.out.println(co);
        
        Collection<String> names = dbm.executeQuery(new ArrayList<String>(), true, 
                //"select first_name + ' ' + last_name as name from customers");              // h2, hsql, sql server
                //"select concat_ws(' ', first_name, last_name) as name from customers");   // mysql
                "select first_name || ' ' || last_name as name from customers");          // postgre, derby, oracle, db2

        for (Object name : names)
          System.out.println(name);
      }
    
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_oracle");
        //CreateDatabase.createHSQLDatabase(dbm);
        
        savesAndUpdates(dbm);

        ejpDatabaseManagerQueries(dbm);

        ejpDatabaseQueries(dbm);
        
        executeQueryAndParameterizedQuery(dbm);
      }
  }
