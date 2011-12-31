package ejp.examples;

import ejp.Database;
import ejp.Database.InParameter;
import ejp.Database.OutParameter;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.Result;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

@SuppressWarnings("unchecked")
public class StoredProcedureOracle 
  {
    public static final int CURSOR = -10;  // oracle.jdbc.OracleTypes.CURSOR
    
    /*
     * Something to play with.
     */
    static void insertData(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate("delete from customers");

        Customer customer = new Customer("deisenhower", "mypasswd5", "Dwight", "Eisenhower", "United States", "deisenhower@unitedstates.gov");

        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Can I have my bust on a dollar, please."));
        customer.getSupport().add(new Support("Response", "Pending", "no phone", "deisenhower@unitedstates.gov", "Yes, but you may have to share it."));
        customer.getSupport().add(new Support("Request", "New", "no phone", "deisenhower@unitedstates.gov", "Share it with who?"));

        customer.getOrders().add(new Order("Dwight D. Eisenhower Dollar", new Integer(100), new Double(1.00), "unverified"));
        customer.getOrders().add(new Order("Susan B. Anthony Dollar", new Integer(10), new Double(1.52), "unverified"));

        dbm.saveObject(customer);
      }
    
    /*
     * Oracle stored procedure.
     */
    static void createProcedure(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate("create or replace procedure getCustomerOrdersAndSupport"
                        + "    (id IN varchar, total OUT decimal, orders_cursor OUT SYS_REFCURSOR, support_cursor OUT SYS_REFCURSOR) is "
                        + "begin "
                        + "    select sum(quantity * price) into total from orders where customer_id = id;"
                        + "    OPEN orders_cursor FOR select * from orders where customer_id = id;"
                        + "    OPEN support_cursor FOR select * from support where customer_id = id;"
                        + "end getCustomerOrdersAndSupport;");
      }

    public static void main(String[] args) throws DatabaseException, SQLException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_oracle");
        
        insertData(dbm);
        createProcedure(dbm);
        
        Database db = dbm.getDatabase();

        try
          {
            Result result = db.storedProcedure("{call getCustomerOrdersAndSupport(?,?,?,?)}", 
                                               new InParameter(1, "deisenhower"),
                                               new OutParameter(2, Types.DOUBLE, 2),
                                               new OutParameter(3, CURSOR),
                                               new OutParameter(4, CURSOR));
            
            /*
             * Map orders from the orders cursor.
             * 
             * Oracle returns cursors as out parameters, so we have to handle the out parameters in the following way.
             */
            Result<Order> orders = (Result<Order>)new Result(db, (ResultSet)((CallableStatement)result.getStatement()).getObject(3), Order.class);
            
            for (Order order : orders)
              System.out.println(order);

            // Map support from the support cursor
            Result<Support> support = (Result<Support>)new Result(db, (ResultSet)((CallableStatement)result.getStatement()).getObject(4), Support.class);

            for (Support s : support)
              System.out.println(s);

            System.out.println("Order total: " + ((CallableStatement)result.getStatement()).getDouble(2));
          }
        finally
          {
            db.close();
          }
      }
  }
