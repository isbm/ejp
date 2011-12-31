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
import ejp.Database;
import ejp.DatabaseManager;
import ejp.DatabaseException;
import ejp.UpdateManager;

public class BatchUpdates 
  {
    static void ejpDatabaseManagerWay(DatabaseManager dbm) throws DatabaseException
      {
        // Inserting customers - with prepared statement caching
        new UpdateManager(dbm) 
          {
            public void run() throws DatabaseException 
              {
                for (int i = 10; i < 10; i++)
                  saveObject(new Customer("customer" + i, "mypasswd" + i, "FirstName" + i, "LastName" + i, "Company" + i, "email" + i + "@company.com"));
              }
          }.executeUpdates();

        /**
         * Inserting customers - batch updating with prepared statement caching
         */
        new UpdateManager(dbm) 
          {
            public void run() throws DatabaseException 
              {
                for (int i = 51; i < 10; i++)
                  saveObject(new Customer("customer" + i, "mypasswd" + i, "FirstName" + i, "LastName" + i, "Company" + i, "email" + i + "@company.com"));
              }
          }.executeBatchUpdates();
      }

    public static void ejpDatabaseWay(DatabaseManager dbm) throws DatabaseException
      {
        Database db = dbm.getDatabase();
        
        try
          {
            // Clean out customers with prepared statement caching
            db.executeUpdate("delete from customers");

            // Inserting customers
            db.saveObject(new Customer("customer0", "mypasswd0", "FirstName0", "LastName0", "Company0", "email0@company.com"));
            db.saveObject(new Customer("customer1", "mypasswd1", "FirstName1", "LastName1", "Company1", "email1@company.com"));
            db.saveObject(new Customer("customer2", "mypasswd2", "FirstName2", "LastName2", "Company2", "email2@company.com"));
            db.saveObject(new Customer("customer3", "mypasswd3", "FirstName3", "LastName3", "Company3", "email3@company.com"));
            db.saveObject(new Customer("customer4", "mypasswd4", "FirstName4", "LastName4", "Company4", "email4@company.com"));

            /**
             * Inserting customers - batch updating with prepared statement caching
             */
            db.beginBatch();

            db.saveObject(new Customer("customer5", "mypasswd5", "FirstName5", "LastName5", "Company5", "email5@company.com"));
            db.saveObject(new Customer("customer6", "mypasswd6", "FirstName6", "LastName6", "Company6", "email6@company.com"));
            db.saveObject(new Customer("customer7", "mypasswd7", "FirstName7", "LastName7", "Company7", "email7@company.com"));
            db.saveObject(new Customer("customer8", "mypasswd8", "FirstName8", "LastName8", "Company8", "email8@company.com"));
            db.saveObject(new Customer("customer9", "mypasswd9", "FirstName9", "LastName9", "Company9", "email9@company.com"));

            db.executeBatch();
            db.endBatch();
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

        ejpDatabaseManagerWay(dbm);
        
        ejpDatabaseWay(dbm);

        for (Customer c : dbm.loadObjects(new ArrayList<Customer>(), Customer.class))
          System.out.print(c);
      }
  }
