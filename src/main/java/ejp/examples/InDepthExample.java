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
import java.util.ArrayList;
import ejp.DatabaseManager;
import ejp.DatabaseException;
import ejp.PersistentClassManager;
import ejp.Result;
import ejp.TransactionManager;
import java.sql.Savepoint;
import java.util.Collection;

/* optional
import ejp.PersistenceManager.ObjectInformation;
import ejp.PersistenceManager.PersistentObject;
import ejp.PersistenceManager.PersistentObjectInterface;
*/

/* uncomment for DBCP (Apache Commons Connection Pooling) - Need commons-pool and commons-dbcp
import org.apache.commons.dbcp.BasicDataSource;
*/

public class InDepthExample
  {
    public InDepthExample(DatabaseManager dbm) throws DatabaseException
      {
        // Clean out customers
        dbm.executeUpdate("delete from customers");

        // Inserting customer with associations
        Customer customer = new Customer("deisenhower", "mypasswd5", "Dwight", "Eisenhower", "United States", "deisenhower@unitedstates.gov");

        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Can I have my bust on a dollar, please."));
        customer.getSupport().add(new Support("Response", "Pending", "no phone", "deisenhower@unitedstates.gov", "Yes, but you may have to share it."));
        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Share it with who?"));

        customer.getOrders().add(new Order("Dwight D. Eisenhower Dollar", new Integer(100), new Double(1.00), "unverified"));
        customer.getOrders().add(new Order("Susan B. Anthony Dollar", new Integer(100), new Double(1.00), "unverified"));

        // Saving within an automatic transaction (covers all relationships)
        dbm.saveObject(customer);

        // Add an associated record and update
        customer.getSupport().add(new Support("Response", "Closed", "no phone", "deisenhower@unitedstates.gov", "You'll have to share with Susan Anthony."));
        dbm.saveObject(customer);

        /*
         * Saving within a transaction manager
         */
        new TransactionManager(dbm)
          {
            public void run() throws DatabaseException 
              {
                // Inserting customer with support and orders
                Customer customer = new Customer("alincoln", "mypasswd1", "Abraham", "Lincoln", null, "alincoln@unitedstates.gov");
                customer.getSupport().add(new Support("Request", "New", "no phone", "alincoln@unitedstates.gov", "Can I have my bust on a penny, please."));
                customer.getOrders().add(new Order("Abraham Lincoln Penny", new Integer(100), new Double(.01), "unverified"));
                saveObject(customer);

                customer = new Customer("tjefferson", "mypasswd2", "Thomas", "Jefferson", "United States", "tjefferson@unitedstates.gov");
                customer.getSupport().add(new Support("Request", "New", "no phone", "tjefferson@unitedstates.gov", "Can I have my bust on a nickel, please."));
                customer.getOrders().add(new Order("Thomas Jefferson Nickel", new Integer(100), new Double(.05), "unverified"));
                saveObject(customer);

                // Insert new customer only
                customer = new Customer("fdroosevelt", "mypasswd3", "Franklin", "Roosevelt", "United States", "fdroosevelt@unitedstates.gov");
                saveObject(customer);

                // Add associated records and update
                customer.getSupport().add(new Support("Request", "New", "no phone", "fdroosevelt@unitedstates.gov", "Can I have my bust on a dime, please."));
                customer.getOrders().add(new Order("Franklin Delano Roosevelt Dime", new Integer(100), new Double(.10), "unverified"));
                saveObject(customer);

                // can still freely use commit, savepoint and rollback

                commit();

                Savepoint savepoint = null;

                if (supportsSavepoints())
                  savepoint = setSavepoint();

                saveObject(customer = new Customer("gwashington", "mypasswd4", "George", "Washington", "United States", "gwashington@unitedstates.gov"));
                customer.getSupport().add(new Support("Request", "New", "no phone", "gwashington@unitedstates.gov", "Can I have my bust on a quarter, please."));
                customer.getOrders().add(new Order("George Washington Quarter", new Integer(100), new Double(.25), "unverified"));
                saveObject(customer);

                if (supportsSavepoints())
                  {
                    System.out.println("rolling back gwashington");
                    
                    rollback(savepoint);
                  }

                // same as this.saveObject
                getDatabase().saveObject(customer = new Customer("jkennedy", "mypasswd4", "John", "Kennedy", "United States", "gwashington@unitedstates.gov"));
                customer.getSupport().add(new Support("Request", "New", "no phone", "gwashington@unitedstates.gov", "Can I have my bust on the half dollar, please."));
                customer.getSupport().add(new Support("Response", "Pending", "no phone", "nobody@unitedstates.gov", "Yes, we'll let you know"));
                customer.getOrders().add(new Order("John Fitzgerald Kennedy Half Dollar", new Integer(100), new Double(.50), "unverified"));
                getDatabase().saveObject(customer);
              }
          }.executeTransaction();

        /*
         * The ejp.DatabaseManager way to load objects
         */

        // Do not load associations
        PersistentClassManager.setIgnoreAssociations(Customer.class, true);

        customer = dbm.loadObject(Customer.class, "where :customerId like 'tjef%'");

        // Load associations separately
        dbm.loadAssociations(customer);

        System.out.println("\ncustomerId = " + customer.getCustomerId());

        // Reset associations
        PersistentClassManager.setIgnoreAssociations(Customer.class, false);

        // or Load based on information contained in objects
        customer = dbm.loadObject(new Customer("tjef%"));
        System.out.println("customerId = " + customer.getCustomerId());

        // or with variable argument parameters
        customer = dbm.loadObject(Customer.class, "where :customerId like ?", "tjef%");
        System.out.println("customerId = " + customer.getCustomerId() + "\n");

        Collection<Customer> c = dbm.loadObjects(new ArrayList<Customer>(), Customer.class);

        for (Customer customer2 : c)
          System.out.println("customerId = " + customer2.getCustomerId());

        System.out.println();

        /*
         * The ejp.Database way to load objects
         */

        Database db = dbm.getDatabase();

        try
          {
            // Query all
            Result<Customer> result = db.queryObject(Customer.class, "order by :lastName");

            // Result is Iterable
            for (Customer customer3 : result)
              {
                System.out.println(customer3);

                // Update a couple of mistakes
                if (customer3.getCustomerId().startsWith("jkennedy"))
                  {
                    customer3.setEmail("jkennedy@unitedstates.gov");

                    for (Support support : customer3.getSupport())
                      if (support.getEmail().startsWith("gwash") || support.getEmail().startsWith("nobody"))
                        support.setEmail("jkennedy@unitedstates.gov");

                    db.saveObject(customer3);
                  }
                else if (customer3.getCustomerId().startsWith("fdroosevelt"))
                  {
                    customer3.setCompanyName(null);

                    db.saveObject(customer3);
                  }
              }

            result.close();

            result = db.queryObject(Customer.class, "order by :lastName");

            // Print and delete
            while (result.hasNext() && (customer = result.next()) != null)
              {
                System.out.println(customer);
                db.deleteObject(customer);
              }

            result.close();
          }
        finally
          {
            // also closes any open results
            db.close();
          }
      }
/*
    static DataSource getDbcpDataSource() throws ClassNotFoundException
      {
        BasicDataSource ds = new BasicDataSource();
        
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        ds.setUsername("sa");
        ds.setPassword("");
        ds.setMaxActive(100);
        ds.setUrl("jdbc:hsqldb:mem:ejp_example");
        
        return ds;
      }
*/
    public static void main(String[] args) throws DatabaseException, ClassNotFoundException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);

        //dbm = DatabaseManager.getDatabaseManager("ejp", 10, getDbcpDataSource());
        //dbm = DatabaseManager.getDatabaseManager("ejp", 10,
        //      "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/ejp_example", "user", "passwd");

        for (int i = 0; i < 1; i++)
          {
            new InDepthExample(dbm);
            System.out.println("count=" + (i + 1));
            System.gc();
          }
      }
  }
