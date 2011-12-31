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

package ejp;

import java.sql.Savepoint;

/**
 * <p>Encloses multiple database calls in a single transaction.
 * 
 * <p>An example of this is:
 * 
 * <pre>
 *   new TransactionManager(dbm) {
 *     public void run() throws DatabaseException 
 *       {
 *         // Inserting individually
 *         getDatabase().saveObject(new Order("alincoln", ...));
 *         // and/or
 *         saveObject(new Order("alincoln", ...));
 *       }
 *   }.executeTransaction();
 * </pre>
 * 
 * <p>While the transaction manager handles beginning the transaction, ending the 
 * transaction, committing the transaction, and rolling back the transaction, 
 * it's also possible to use save points, rollback, and commit at any 
 * point within.
 */
public abstract class TransactionManager
  {
    Database database;
    boolean closeDatabase;

    /**
     * Construct a TransactionManager with a database manager.
     *
     * @param databaseManager a ejp.DatabaseManager instance
     */
    public TransactionManager(DatabaseManager databaseManager) throws DatabaseException
      {
        this.database = databaseManager.getDatabase();
        closeDatabase = true;
      }

    /**
     * Construct a TransactionManager with a database.
     *
     * @param database a ejp.Database instance
     */
    public TransactionManager(Database database) throws DatabaseException
      {
        this.database = database;
      }

    /**
     * Returns the database being used for the transaction.  This is useable for
     * any database functionality, including commits, Savepoints, and rollbacks.
     */
    public Database getDatabase() { return database; }

    /**
     * Override this method with your own.
     */
    public abstract void run() throws Exception;
    
    /**
     * Calls the ejp.Database version.
     */
    public int saveObject(Object object) throws DatabaseException
      {
        return database.saveObject(object);
      }
    
    /**
     * Calls the ejp.Database version.
     */
    public int saveObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        return database.saveObject(object, externalClauses, externalClausesParameters);
      }
    
    /**
     * Calls the ejp.Database version.
     */
    public int deleteObject(Object object) throws DatabaseException
      {
        return database.deleteObject(object);
      }
    
    /**
     * Calls the ejp.Database version.
     */
    public int deleteObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        return database.deleteObject(object, externalClauses, externalClausesParameters);
      }
    
    /**
     * Calls the ejp.Database version.
     */
    public void commit() throws DatabaseException { database.commit(); }
    
    /**
     * Calls the ejp.Database version.
     */
    public void rollback() throws DatabaseException { database.rollback(); }
    
    /**
     * Calls the ejp.Database version.
     */
    public void rollback(Savepoint savepoint) throws DatabaseException { database.rollback(savepoint); }
    
    /**
     * Calls the ejp.Database version.
     */
    public Savepoint setSavepoint() throws DatabaseException { return database.setSavepoint(); }
    
    /**
     * Calls the ejp.Database version.
     */
    public Savepoint setSavepoint(String name) throws DatabaseException { return database.setSavepoint(name); }
   
    /**
     * Calls the ejp.MetaData version.
     */
    public boolean supportsSavepoints() throws DatabaseException { return database.getMetaData().supportsSavepoints(); }
    
    /**
     * Call this method to execute the transaction on the updates defined in the run() method.  
     * If no exceptions occur the transaction will be commited.  Othwerwise, the 
     * transaction will be rolled back.
     */
    public void executeTransaction() throws DatabaseException
      {
        try
          {
            database.beginTransaction();
            
            run();
          }
        catch (Exception e)
          {
            database.rollback();
            
            throw new DatabaseException("Transaction was rolled back", e);
          }
        finally
          {
            database.endTransaction();
            
            if (closeDatabase)
              database.close();
          }
      }
  }
