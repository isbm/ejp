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
import java.util.ArrayList;
import java.util.Collection;

/**
 * DatabaseManager is the highest level of EJP.  It's also the easiest level of EJP.  
 * DatabaseManager allows you to do anything with your database that you like.
 * You can insert, update, query, and delete objects.  You can easily execute 
 * native sql statements.
 */
@SuppressWarnings("unchecked")
public class EjpDatabaseManagerExamples 
  {
    static void addData(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = new Customer("jsmith", "", "John", "Smith", "ABCProducts", "jsmith@abcproducts.com");
        
        dbm.saveObject(customer);

        customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
        customer.getOrders().add(new Order("Candle Holder", 2, 10.95, "new"));

        customer.getSupport().add(new Support("request", "new", "123-123-1234", "jsmith@abcproducts.com", "Please send ASAP"));
        
        dbm.saveObject(customer);
        
        customer = new Customer("jpeters", "", "Joe", "Peters", "ABCProducts", "jpeters@abcproducts.com");
        
        customer.getOrders().add(new Order("Scented Candles", 10, 1.25, "new"));
        customer.getOrders().add(new Order("Candle Holder", 2, 10.95, "new"));
        
        dbm.saveObject(customer);
      }

    static void loadingObjects(DatabaseManager dbm) throws DatabaseException
      {
        System.out.println("objectQueries");

        Customer customer = dbm.loadObject(new Customer("jsmith"));
        System.out.println(customer);
        
        customer = dbm.loadObject(Customer.class, "where :customerId = ?", "jsmith");
        System.out.println(customer);
        
        Collection<Customer> customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class);

        for (Customer c : customers)
          System.out.println(c);
      }
    
    static void loadingObjectsFromNativeQueries(DatabaseManager dbm) throws DatabaseException
      {
        System.out.println("nativeQueries");
        
        Collection<Customer> customers = dbm.executeQuery(new ArrayList<Customer>(), Customer.class, "select * from customers;");
        for (Customer c : customers)
          System.out.println(c);
        
        customers = dbm.parameterizedQuery(new ArrayList<Customer>(), Customer.class, "select * from customers where customer_id = ?;", "jsmith");
        for (Customer c : customers)
          System.out.println(c);
        
        Collection<String> ids = dbm.executeQuery(new ArrayList<String>(), true, "select customer_id from customers;");
        for (String id : ids)
          System.out.println(id);
        
        Collection<Object[]> namesAndEmails = dbm.executeQuery(new ArrayList<Object[]>(), true, "select first_name + ' ' + last_name as name, email from customers;");
        for (Object[] nameAndEmail : namesAndEmails)
          System.out.println(nameAndEmail[0] + ", " + nameAndEmail[1]);
      }
    
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);
        
        addData(dbm);
        
        loadingObjects(dbm);
        
        loadingObjectsFromNativeQueries(dbm);
      }
  }
