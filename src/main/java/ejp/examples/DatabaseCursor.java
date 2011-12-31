package ejp.examples;

import ejp.Database;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.Result;

public class DatabaseCursor 
  {
    /**
     * Saves and updates are the same with ejp.DatabaseManager and ejp.Database.
     */
    static void savesAndUpdates(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate("delete from customers");
        
        for (int i = 0; i < 10; i++)
          {
            Customer customer = new Customer("ssmith" + i, "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
            customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));

            dbm.saveObject(customer);
          }
      }
    
    /**
     * Allthough ejp.DatabaseManager methods simply wrap ejp.Database 
     * functionality for one line ease of use, the real power, and 
     * the heart of EJP, is ejp.Database.  
     * 
     * The following demonstrates a query with ejp.Database.  ejp.Database 
     * queries can be performed with queryObject, execute, executeQuery, 
     * parameterizedQuery, and storedProcedure.  
     * 
     * All queries with ejp.Database return an ejp.Result object, which wraps 
     * a java.sql.ResultSet.  ejp.Result is a database cursor and this is 
     * what you want when handling large query results.  Since a cursor is a 
     * database resource, you will want to close ejp.Result when you are done. 
     * Closing ejp.Database also closes its ejp.Results.
     */
    static void databaseCursor(DatabaseManager dbm) throws DatabaseException
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
    
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        
        CreateDatabase.createHSQLDatabase(dbm);
        
        savesAndUpdates(dbm);

        databaseCursor(dbm);
      }
  }
