package ejp.examples;

import ejp.Database;
import ejp.Database.InParameter;
import ejp.Database.OutParameter;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.Result;
import ejp.TransactionManager;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

/* Easy Java Persistence and JDBC Toolbox
 * 
 * Easy Java Persistence (EJP) is a configuration and annotation free Java persistence Framework 
 * that allows you to use normal (true) POJO classes to interact with your database.  
 *
 * EJP is an A-O/RM (automatic object/relational mapping) persistence framework.  Any class can 
 * be mapped to data returned from queries provided it has methods that can be matched to columns 
 * in the result data. And any class that can be matched to a table in your database can be mapped, 
 * saved (insert), and persisted, allowing for more saves (updates).  Objects don't belong to any 
 * EJP instance, and they never expire.
 * 
 * ejp.DatabaseManager is the starting point with EJP.  ejp.DatabaseManager wraps functionality 
 * from ejp.Database and ejp.Result to provide simple one-line of code methods to most of EJP's 
 * functionality.  ejp.Databasemanager is the highest and easiest level of EJP, while ejp.Database 
 * and ejp.Result are the next highest level, and are also very easy to work with.
 * 
 * ejp.DatabaseManager is thread safe when connection pooling is in use.  ejp.Database is also thread 
 * safe when connection pooling is used, and you make sure to use one ejp.Database (dbm.getDatabase())
 * instance per thread.
 *
 * EJP also manages all associations automatically with collections (automatically lazy loaded), 
 * arrays, and single instance objects.
 * 
 * EJP is also a JDBC toolbox.  EJP provides you the ability to do anything you can do with JDBC 
 * and makes it a whole lot easier.  You can perform simple and complex queries, EJP allows for 
 * simple statements (executeQuery/executeUpdate) and/or prepared statements 
 * (parameterizedquery/parameterizedUpdate).  You can call stored procedures and handle multiple 
 * results.
 */
@SuppressWarnings("unchecked")
public class IntroducingEjp 
  {
    /* ejp.DatabaseManager
     * 
     * The main difference between ejp.DatabaseManager and ejp.Database is that ejp.DatabaseManager returns 
     * objects and collections from queries, while ejp.Database returns ejp.Result (a database cursor) from 
     * queries.  
     * 
     * ejp.Result is a bidirectional database cursor (wraps java.sql.ResultSet), and can be 
     * used to iterate trough huge query result sets.  ejp.Result also implements ListIterator and Iterable, 
     * so you can use next() and previous(), and you can use ejp.Result in a foreach statement.  next() and 
     * previous() return newly created instances of whatever class is associated with the ejp.Result instance.  
     * Since ejp.Result is a database resource, it needs to be closed as soon as you are done with it.  All 
     * ejp.Results are closed automatically when closing ejp.Database.
     *
     * There are multiple ways to obtain an ejp.DatabaseManager.  You can define the following:
     * 
     * <pre>
     *       <databases>
     *           <database name="" poolSize="" useJndi="" driver="" url="" username="" password="" catalogPattern="" schemaPattern="" />
     *       </databases>
     * </pre>
     * 
     * in a file named "databases.xml" that's locatable in your classpath.  You can call one of ejp.DatabaseManagers's 
     * getDatabasemanager() methods, or you can simply create a new instance of ejp.DatabaseManager.
     * 
     * ejp.DatabaseManager excepts a name from the databases.xml file, a javax.sql.DataSource, a JNDI resource name, or driver and url information.
     */
    static DatabaseManager getTheDatabaseManager()
      {
        // return DatabaseManager.getDatabaseManager("nameFromDatabase.xml");
        // return DatabaseManager.getDatabaseManager("name", poolSize, dataSource);
        // return DatabaseManager.getDatabaseManager("name", poolSize, jndiUrl);
        // return DatabaseManager.getDatabaseManager("name", poolSize, "driver", "url", "user", "passwd");
        
        // Returns a databases.xml defined database for HSQLDB
        return DatabaseManager.getDatabaseManager("ejp_hsql");
      }
    
    /* Inserting and Updating
     * 
     * Inserting and updating objects, with both ejpDatabaseManager and ejp.Database, is done with saveObject().  
     * SaveObject() always knows weather it's an insert or an update, so there's only one method for saving.  You
     * can also use executeUpdate() and parameterizedUpdate(), if you want to do it with plain SQL.
     */
    static void insertingAndUpdatingObjects(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = new Customer("jjohnson", "mypasswd5", "John", "Johnson", "ABCProducts", "jjohnson@abcproducts.com");
        customer.getOrders().add(new Order("NFL Denver Broncos Coffee Cup", 100, 1.00, "unverified"));

        dbm.saveObject(customer);
        
        customer = new Customer("ssmith", "mypasswd", "Scott", "Smith", "ABCProducts", "ssmith@abcproducts.com");
        customer.getOrders().add(new Order("NFL Seattle Seahawks Team T-Shirts", 150, 12.00, "unverified"));
        
        dbm.saveObject(customer);

        customer.setCompanyName("ACB Products");
        customer.getOrders().add(new Order("NFL Greenbay Packers Team T-Shirts", 150, 12.00, "verified"));

        dbm.saveObject(customer);

        // the old fashion way
        dbm.parameterizedUpdate("insert into customers (customer_id, password, first_name, last_name, company_name, email) values(?,?,?,?,?,?)", 
                                "jstevens", "mypasswd5", "John", "Stevens", "ABCProducts", "jstevens@abcproducts.com");
        
        dbm.parameterizedUpdate("insert into orders (customer_id, product, quantity, price, status) values(?,?,?,?,?)", 
                                "jstevens", "NFL Dallas Cowboys Coffee Cup", 100, 1.00, "unverified");
      }
    
    /* Queries
     * 
     * Retrieving data from your database is done with loadObject() for a single object, and loadObjects() 
     * for a collection.  You can also use executeQuery(), and parameterizedQuery().
     */
    static void databaseManagerQueries(DatabaseManager dbm) throws DatabaseException
      {
        Customer customer = dbm.loadObject(new Customer("jjohnson"));
        System.out.println(customer);

        customer = dbm.loadObject(Customer.class, "where customer_id = ?", "jjohnson");
        System.out.println(customer);
        
        for (Customer c : dbm.loadObjects(new ArrayList<Customer>(), Customer.class))
          System.out.println(c);
          
        Collection<Customer> customers = dbm.loadObjects(new ArrayList<Customer>(), Customer.class, 
                "from customers c, orders o where c.customer_id = o.customer_id and o.status = ?", "verified");
        
        for (Customer c : customers)
          System.out.println(c);
        
        customers = dbm.executeQuery(new ArrayList<Customer>(), Customer.class, 
                "select * from customers");

        for (Customer c : customers)
          System.out.println(c);

        customers = dbm.parameterizedQuery(new ArrayList<Customer>(), Customer.class, 
                "select c.* from customers c, orders o where c.customer_id = o.customer_id and o.status = ?", "verified");

        for (Customer c : customers)
          System.out.println(c);
      }
    
    /* ejp.Database
     * 
     * As mentioned above, ejp.Database and ejp.DatabaseManager have similar functionality, but ejp.Database 
     * returns ejp.Result (a database cursor) from queries.  
     * 
     * As mentioned ejp.Result is a bidirectional database cursor.  ejp.Result implements ListIterator
     * and Iterable, so you can use next() and previous(), and you can use it in a foreach statement.  next() and 
     * previous() return newly created instances of whatever class is associated with the ejp.Result instance.
     * Since ejp.Result is a database resource, it needs to be closed as soon as you are done with it.  All 
     * ejp.Results are closed automatically when closing ejp.Database.
     * 
     * ejp.Database also supports stored procedures and multiple result sets.
     */
    
    static void theEjpDatabaseWay(DatabaseManager dbm) throws DatabaseException
      {
        Database db = dbm.getDatabase();
        
        try
          {
            // Optional - makes rows updateable
            // db.setResultSetConcurrency(ResultSet.CONCUR_UPDATABLE);
            // db.setResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE);
            
            Result<Customer> result = db.queryObject(new Customer("jjohnson"));
            
            if (result.hasNext())
              {
                Customer customer = result.next();
                
                customer.setPassword("newPasswd");
                db.saveObject(customer);
                // or we could just update the column
                // result.setColumnValue("password", "newPasswd");
                // result.updateRow();
              }

            result = db.queryObject(Customer.class);
            
            for (Customer c : result)
              System.out.println(c);
          }
        finally 
          {
            // Always close as soon as possible when connection pooled
            db.close(); 
          } 
      }
    
    /* Stored Procedures
     * 
     * EJP even makes working with stored procedures and functions easy to handle.  Simply use 
     * the JDBC stored procedure syntax:
     * 
     * <pre>
     *       {?= call <procedure-name>[(?, ?, ?, ...)]}
     *       {call <procedure-name>[(?, ?, ?, ...)]}
     * </pre>
     * 
     * and one or more of InParameter, OutParameter, and InOutParameter.  storedprocedure() 
     * returns a Result that may have one or more results and/or update counts that can be 
     * accessed with getMoreResults(), isUpdateCount(), getUpdateCount().
     * 
     * When done, you can access the out parameters by retrieving the CallableStatement with
     * getStatement().
     */
    static void storedProcedures(DatabaseManager dbm) throws DatabaseException, SQLException
      {
        Database db = dbm.getDatabase();
        
        try
          {
            Result result = db.storedProcedure("{call getCustomerOrdersAndSupport(?,?)}", 
                                               new InParameter(1, "deisenhower"),
                                               new OutParameter(2, Types.DOUBLE, 2));

            // Map orders from the orders cursor
            for (Order order : (Result<Order>)result.getResultSetWithClass(Order.class))
              {
                order.setStatus("verified");
                db.saveObject(order);
                
                System.out.println(order);
              }
            
            result.getMoreResults();

            // Map support from the support cursor
            for (Support s : (Result<Support>)result.getResultSetWithClass(Support.class))
              System.out.println(s);

            System.out.println("Order total: " + ((CallableStatement)result.getStatement()).getDouble(2));
          }
        finally { db.close(); }
      }
    
    /* Transactions
     * 
     * With ejp.DatabaseManager, saveObject() is always enclosed in a transaction.  The object being 
     * saved, and its associations, are all inclosed in the transaction.  
     * 
     * With ejp.Database, transactions are automatic when auto commit is true (the default state).  Otherwise,
     * saveObject(), executeUpdate(), and parameterizedUpdated() will all be enclosed in the transaction that
     * was started when auto commit became false (beginTransaction()).
     * 
     * when you need more transaction control you can use ejp.TransactionManager or you can use 
     * ejp.Database's beginTransaction(), commit(), rollback(), setSavepoint(), and endTransaction().
     * 
     * When using ejp.TransactionManager, you have to use ejp.TransactionManager's methods saveObject(), commit(), 
     * rollback(), setSavepoint().  You can also get the handle to the ejp.Database that is being used with 
     * getDatabase().
     */
    static void transactions(DatabaseManager dbm) throws DatabaseException
      {
        new TransactionManager(dbm) 
          {
            public void run()
              {
                // do your work
              }
          }.executeTransaction();
        
        // or
        Database db = dbm.getDatabase();
        
        try
          {
            db.beginTransaction();
            // do your work
            db.endTransaction();
          } 
        finally { db.close(); }
      }
    
    /*
     * Obviously, we're just touching upon the functionality and all the things you can do with EJP, 
     * but the above is the majority of what you would do from day to day and project to project.
     */
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = getTheDatabaseManager();

        // creates an in-memory database for us to play with
        CreateDatabase.createHSQLDatabase(dbm);
        
        insertingAndUpdatingObjects(dbm);
        
        databaseManagerQueries(dbm);
        
        theEjpDatabaseWay(dbm);
      }
  }
