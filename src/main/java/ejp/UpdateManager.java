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

/**
 * <p>Encloses multiple database updates using prepared statements and/or 
 * batch update processing.
 * 
 * <p>An example of this is:
 * 
 * <pre>
 *   new UpdateManager(dbm) {
 *     public void run() throws DatabaseException 
 *       {
 *         // Inserting individually
 *         getDatabase().saveObject(new Order("alincoln", ...));
 *         // and/or
 *         saveObject(new Order("alincoln", ...));
 *       }
 *   }.executeUpdates();
 * or:
 *   }.executeBatchUpdates();
 * </pre>
 * 
 * <p>Since the update manager extends the transaction manager,
 * it's possible to use save points, rollback, and commit at any 
 * point within.
 */
public abstract class UpdateManager extends TransactionManager
  {
    /**
     * Construct an UpdateManager with a database manager.
     *
     * @param databaseManager a ejp.DatabaseManager instance
     */
    public UpdateManager(DatabaseManager databaseManager) throws DatabaseException
      {
        super(databaseManager);
      }

    /**
     * Construct an UpdateManager with a database.
     *
     * @param database a ejp.Database instance
     */
    public UpdateManager(Database database) throws DatabaseException
      {
        super(database);
      }
    
    /**
     * Call this method to execute the updates defined in the run() method.  
     * This method also starts a transaction. If no 
     * exceptions occur the transaction will be commited.  Othwerwise, the 
     * transaction will be rolled back.
     */
    public void executeUpdates() throws DatabaseException
      {
        super.executeTransaction();
      }

    /**
     * Call this method to execute the batch updates defined in the run() method.  
     * This method also starts a transaction. If no 
     * exceptions occur the transaction will be commited.  Othwerwise, the 
     * transaction will be rolled back.
     * 
     * @return the update counts for the batch processing
     */
    public Integer[] executeBatchUpdates() throws DatabaseException
      {
        Integer[] updateCounts = null;
        
        try
          {
            database.beginTransaction();
            database.beginBatch();
            
            run();
          }
        catch (Exception e)
          {
            database.endBatch();
            database.rollback();
            
            throw new DatabaseException("Transaction was rolled back", e);
          }
        finally
          {
            database.executeBatch();
            
            updateCounts = database.getBatchUpdateCounts();
            
            database.endBatch();
            database.endTransaction();
            
            if (closeDatabase)
              database.close();
          }
            
        return updateCounts;
      }
  }
