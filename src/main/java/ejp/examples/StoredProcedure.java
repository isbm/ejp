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
// import org.hsqldb.types.Types;

@SuppressWarnings("unchecked")
public class StoredProcedure 
  {
    
    public static final int DOUBLE = 8;  // org.hsqldb.types.Types.DOUBLE
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
     * MySql stored procedure.
     */
    static void createProcedure(DatabaseManager dbm) throws DatabaseException
      {
        dbm.executeUpdate("DROP PROCEDURE IF EXISTS getCustomerOrdersAndSupport");
        
        dbm.executeUpdate("CREATE PROCEDURE getCustomerOrdersAndSupport(in id varchar(20), out total decimal(5,2)) "
                        + "BEGIN "
                        + "select sum(quantity * price) into total from orders where customer_id = id; "
                        + "select * from orders where customer_id = id; "
                        + "select * from support where customer_id = id; "
                        + "END");
      }
    
    public static void main(String[] args) throws DatabaseException, SQLException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_mysql");
        
        insertData(dbm);
        createProcedure(dbm);
        
        Database db = dbm.getDatabase();

        try
          {
            Result result = db.storedProcedure("{call getCustomerOrdersAndSupport(?,?)}", 
                                               new InParameter(1, "deisenhower"),
                                               new OutParameter(2, DOUBLE, 2));

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
        finally
          {
            db.close();
          }
      }
  }
