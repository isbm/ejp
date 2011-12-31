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

import java.util.ArrayList;
import ejp.DatabaseManager;
import ejp.DatabaseException;
import java.util.Collection;

public class SimpleExample
  {
    static void simpleExample(DatabaseManager dbm) throws DatabaseException
      {
        // Clean out customers
        dbm.executeUpdate("delete from customers");

        // Inserting customer with associations
        Customer customer = new Customer("deisenhower", "mypasswd5", "Dwight", "Eisenhower", "United States", "deisenhower@unitedstates.gov");
        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Can I have my bust on a dollar, please."));
        customer.getOrders().add(new Order("Dwight D. Eisenhower Dollar", new Integer(100), new Double(1.00), "unverified"));

        // Saving within an automatic transaction (covers all relationships)
        dbm.saveObject(customer);

        // Load based on information contained in classes
        customer = dbm.loadObject(Customer.class, "where :customerId = ?", "deisenhower");
        System.out.println("\ncustomerId = " + customer.getCustomerId() + ", last name = " + customer.getLastName());
        
        for (Order order : customer.getOrders())
          System.out.println("Order: " + order.getProduct());
        
        for (Support support : customer.getSupport())
          System.out.println("Support:" + support.getRequest());

        System.out.println();
        
        // Load a collection of objects from the database
        Collection<Customer> customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class);

        for (Customer customer2 : customers)
          {
            System.out.println("customerId = " + customer2.getCustomerId() + ", last name = " + customer2.getLastName());

            for (Order order : customer2.getOrders())
              System.out.println("Order: " + order.getProduct());

            for (Support support : customer2.getSupport())
              System.out.println("Support:" + support.getRequest());
          }
      }

    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);

        //dbm = DatabaseManager.getDatabaseManager("ejp", 10,
        //      "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/ejp_example", "user", "passwd");

        simpleExample(dbm);
      }
  }
