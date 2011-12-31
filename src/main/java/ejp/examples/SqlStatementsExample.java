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

import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.SqlStatements;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This example simply shows the purpose of ejp.utilities.SqlStatements.  
 * You can load your SQL fragments/statements from a table, a properties 
 * file, or via a map.  It's a very simple key/value pair, that allows 
 * you to keep from hard coding SQL fragements/statements everywhere.
 */
public class SqlStatementsExample
  {
    SqlStatements sql = new SqlStatements();
    
    /**
     * Something to play with
     */
    void savesAndUpdates(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate(sql.getSqlStatement("deleteCustomers"));
        
        // The old fashioned SQL way
        dbm.parameterizedUpdate(sql.getSqlStatement("insertCustomer"), 
                                "jjohnson", "mypasswd5", "John", "Johnson", "ABCProducts", "jjohnson@abcproducts.com");
        
        dbm.parameterizedUpdate(sql.getSqlStatement("insertOrder"), 
                                "jjohnson", "NFL Broncos Coffee Cup", 100, 1.00, "unverified");

        // The modern object oriented way
        Customer customer = new Customer("ssmith", "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
        customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));

        dbm.saveObject(customer);
      }
    
    void ejpDatabaseManagerQueries(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = dbm.loadObject(Customer.class, sql.getSqlStatement("searchCustomer"), "jjohnson");
        System.out.println(customer);
        
        Collection<Customer> customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class, 
                sql.getSqlStatement("fromCustomersAndOrders"), "unverified");
        
        for (Customer c : customers)
          System.out.println(c);
      }
    
    void executeQueryAndParameterizedQuery(DatabaseManager dbm) throws DatabaseException
      {
        Collection<Customer> customers = dbm.parameterizedQuery(new ArrayList<Customer>(), Customer.class, 
                sql.getSqlStatement("selectFromCustomers"), "unverified");

        for (Customer c : customers)
          System.out.println(c);
      }
    
    void runExample(DatabaseManager dbm) throws DatabaseException
      {
        /*
         * A "hard coded" map.  Most likely you will want this in a table or 
         * an external properties file.
         */
        Map<String, String> m = new HashMap<String, String>();
        m.put("deleteCustomers", "delete from customers");
        m.put("insertCustomer", "insert into customers (customer_id, password, first_name, last_name, company_name, email) values(?,?,?,?,?,?)");
        m.put("insertOrder", "insert into orders (customer_id, product, quantity, price, status) values(?,?,?,?,?)");
        m.put("searchCustomer", "where customer_id = ?");
        m.put("fromCustomersAndOrders", "from customers c, orders o where c.customer_id = o.customer_id and o.status = ?");
        m.put("selectFromCustomers", "select c.* from customers c, orders o where c.customer_id = o.customer_id and o.status = ?");
        
        sql.setSqlStatements(m);
        // or
        // sql.loadSqlStatements(dbm, "select sql_id, sql_statement from sql_statements");
        // sql.loadSqlStatements(new File("path_to_properties_file"));

        savesAndUpdates(dbm);

        ejpDatabaseManagerQueries(dbm);

        executeQueryAndParameterizedQuery(dbm);
      }
    
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);
        
        new SqlStatementsExample().runExample(dbm);
      }
  }
