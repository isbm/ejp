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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ejp.utilities.StringUtils;
import ejp.ORMSupport.NullValue;
import ejp.interfaces.AsciiStream;
import ejp.interfaces.BinaryStream;
import ejp.interfaces.CharacterStream;
import java.sql.CallableStatement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * The center of ejp, the Database class provides a seamless integration 
 * of the JDBC classes Connection, Statement, and ResultSet,  while also 
 * providing object/relational database access. Use ejp.DatabaseManager 
 * to obtain an instance of ejp.Database.
 */

@SuppressWarnings("unchecked")
public final class Database
  {
    private static Logger logger = LoggerFactory.getLogger(Database.class);
    private String cursorName, databaseName, catalogPattern, schemaPattern, querySql, updateSql, callableSql;
    private List<Result> resultsList;
    private List<Integer> batchUpdateCounts;
    private List<Statement> batchExecuteOrder;
    private DatabaseManager databaseManager;
    private Statement queryStatement, updateStatement, callableStatement;
    private Map<String, Statement> batchStatements;
    private Connection connection;
    private boolean isClosed, isBatch, automaticTransactions = true;
    private Boolean escapeProcessing, ignoreAssociations;
    private Integer queryTimeout, fetchDirection, fetchSize, maxRows, maxFieldSize, resultSetType, resultSetConcurrency;

    /* Non-public access ************************************************************/

    Database(DatabaseManager databaseManager)
      {
        this.databaseManager = databaseManager;
      }

    DatabaseManager getDatabaseManager() { return databaseManager; }

    DatabaseManager.PersistentClassManager getPersistentClassManager() 
      {
        if (databaseManager != null)
          return databaseManager.persistentClassManager;
 
        return null;
      }
    
    boolean isBatch() { return isBatch; }

    void setDatabaseName(String databaseName) { this.databaseName = databaseName; }

    void initDatabase()
      {
        cursorName = querySql = updateSql = callableSql = null;
        queryStatement = updateStatement = callableStatement = null;
        resultsList = new ArrayList<Result>();
        batchUpdateCounts = null;
        batchStatements = null;
        batchExecuteOrder = null;
        escapeProcessing = ignoreAssociations = null;
        isClosed = isBatch = false;
        queryTimeout = fetchDirection = fetchSize = maxRows = maxFieldSize = resultSetType = resultSetConcurrency = null;
      }

    void setConnection(Connection connection)
      {
        this.connection = connection; 
      }

    void closeResult(Result result)
      {
        try
          {
            ResultSet resultSet = result.getResultSet();
            Statement statement = resultSet.getStatement();

            if (statement == queryStatement)
              resultSet.close();
            else
              statement.close();
          }
        catch (Exception e) { } // don't care.  JDBC spec says close can be called multiple times, but Resin was complaining.
      }
    
    void closeQueryStatement()
      {
        if (queryStatement != null)
          {
            try
              {
                queryStatement.close();
              }
            catch (Exception e) { } // don't care.  JDBC spec says close can be called multiple times, but Resin was complaining.

            queryStatement = null;
          }
      }
    
    void closeUpdateStatement()
      {
        if (updateStatement != null)
          {
            try
              {
                updateStatement.close();
              }
            catch (Exception e) { } // don't care.  JDBC spec says close can be called multiple times, but Resin was complaining.

            updateStatement = null;
          }
      }

    String getCatalogPattern() { return catalogPattern; }

    String getSchemaPattern() { return schemaPattern; }
    
    /* End Non-public access ************************************************************/
    
   /**
     * Returns the name of the current database handler.
     *
     * @return a string representing the database handler name
     */
    
    public String getDatabaseName() { return databaseName; }

    /**
     * Returns the database connection.
     * 
     * @return the JDBC connection instance
     */
    
    public Connection getConnection()
      {
        return connection;
      }

    /**
     * Returns the database meta data associated with the current database.  
     * The MetaData class can be used to access information about tables in 
     * the database.  It can also be used to add table and column mapping.
     * 
     * @return an instance of MetaData
     *
     * @throws DatabaseException
     */
    
    public MetaData getMetaData() throws DatabaseException 
      {
        try
          {
            return MetaData.getMetaData(connection); 
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Sets limits on the meta data table information returned.  Defining 
     * catalogPattern and schemaPattern can help reduce the amount of time
     * spent loading table information.  With some databases, it is absolutely
     * needed.  This can also be set with databases.xml and/or DatabaseManager
     * constructors.
     * 
     * @param catalogPattern the catalogPattern (can contain SQL wildcards)
     * @param schemaPattern the schemaPattern (can contain SQL wildcards)
     * @deprecated Use setCatalog() and/or setSchema()
     */
    @Deprecated
    public void setMetaDataLimits(String catalogPattern, String schemaPattern)
      {
        logger.debug("Limiting meta data with catalog = {}, schema = {}", catalogPattern, schemaPattern);
        
        this.catalogPattern = catalogPattern;
        this.schemaPattern = schemaPattern;
      }
    
    /**
     * Set the catalog pattern to use.
     * 
     * @param catalogPattern the catalogPattern (can contain SQL wildcards)
     */
    public void setCatalogPattern(String catalogPattern)
      {
        logger.debug("Limiting meta data with catalog = {}", catalogPattern);
        
        this.catalogPattern = catalogPattern;
      }
    
    /**
     * Set the schema pattern to use.
     * 
     * @param schemaPattern the catalogPattern (can contain SQL wildcards)
     */
    public void setSchemaPattern(String schemaPattern)
      {
        logger.debug("Limiting meta data with schema = {}", schemaPattern);
        
        this.schemaPattern = schemaPattern;
      }
    
    /**
     * Closes the Database (returns pooled connections to the pool).
     * 
     * @throws DatabaseException
     */
    
    public void close()
      {
        logger.debug("closing database");

        try
          {
            endBatch();

            for (Result result : resultsList)
              result.close();

            closeQueryStatement();
            closeUpdateStatement();
          }
        catch (Exception e)
          {
            logger.error(e.toString(), e);
          }
        finally
          {
            if (databaseManager != null)
              databaseManager.releaseDatabase(this);
          }
        
        isClosed = true;
      }
    
    /**
     * Returns true if the database is closed, false otherwise.
     *
     * @return true or false
     */
    
    public boolean isClosed() { return isClosed; }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */

    public void setFetchDirection(Integer direction) 
      {
        logger.debug("Setting fetch direction to {}", direction);

        this.fetchDirection = direction;
      }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */

    public void setFetchSize(Integer fetchSize)
      {
        logger.debug("Setting fetch size to {}", fetchSize);
        
        this.fetchSize = fetchSize; 
      }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setMaxRows(Integer maximumResultRows)
      {
        logger.debug("Setting maximum result rows to {}", maximumResultRows);
        
        this.maxRows = maximumResultRows; 
      }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setMaxFieldSize(Integer max)
      {
        logger.debug("Setting max field size to {}", max);

        this.maxFieldSize = max;
      }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setCursorName(String name)
      {
        logger.debug("Setting cursor name to {}", name);

        this.cursorName = name;
      }

    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setEscapeProcessing(Boolean enable) 
      {
        logger.debug("Setting escape processing to {}", enable);

        this.escapeProcessing = enable;
      }

    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setQueryTimeout(Integer seconds)
      {
        logger.debug("Setting query timeout to {}", seconds);

        this.queryTimeout = seconds;
      }
    
    /**
     * Defaults to ResultSet.TYPE_SCROLL_INSENSITIVE.
     * 
     * @see java.sql.Connection
     * @see java.sql.ResultSet
     */

    public void setResultSetType(Integer resultSetType)
      {
        logger.debug("Setting result set type to {}", resultSetType);
        
        this.resultSetType = resultSetType;
      }

    /**
     * Defaults to ResultSet.CONCUR_READ_ONLY.
     * 
     * @see java.sql.Connection
     * @see java.sql.ResultSet
     */
    
    public void setResultSetConcurrency(Integer resultSetConcurrency)
      {
        logger.debug("Setting result set concurrency to {}", resultSetConcurrency);
        
        this.resultSetConcurrency = resultSetConcurrency;
      }

    Statement getStatement() throws DatabaseException
      {
        try
          {
            logger.debug("Creating statement for querying: resultSetType = {}, resultSetConcurrency = {}", resultSetType, resultSetConcurrency);

            Statement statement = connection.createStatement(resultSetType != null ? resultSetType : ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                             resultSetConcurrency != null ? resultSetConcurrency : ResultSet.CONCUR_READ_ONLY);

            if (fetchSize != null)
              statement.setFetchSize(fetchSize);

            if (maxRows != null)
              statement.setMaxRows(maxRows);
          
            if (cursorName != null)
              statement.setCursorName(cursorName);
            
            if (escapeProcessing != null)
              statement.setEscapeProcessing(escapeProcessing);

            if (maxFieldSize != null)
              statement.setMaxFieldSize(maxFieldSize);
            
            if (queryTimeout != null)
              statement.setQueryTimeout(queryTimeout);
            
            if (fetchDirection != null)
              statement.setFetchDirection(fetchDirection);

            return statement;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    Statement getStatementForUpdate() throws DatabaseException
      {
        try
          {
            logger.debug("Creating statement for updating");

            if (isBatch)
              {
                if ((updateStatement = batchStatements.get("simpleStatement")) != null)
                  return updateStatement;
              }
            else closeUpdateStatement();

            updateStatement = getConnection().createStatement();

            if (isBatch)
              {
                batchStatements.put("simpleStatement", updateStatement);
                batchExecuteOrder.add(updateStatement);
              }
            
            return updateStatement;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    CallableStatement getCallableStatement(String sql) throws DatabaseException
      {
        if (sql == null || sql.length() == 0)
          throw new DatabaseException(DatabaseException.SQL_STATEMENT_NULL);

        try
          {
            if (callableStatement != null && callableStatement instanceof CallableStatement && sql.equals(callableSql))
              return (CallableStatement)callableStatement;

            logger.debug("Creating callable statement:");
            logger.debug("sql = {}", sql);
            logger.debug("resultSetType = {}", resultSetType);
            logger.debug("resultSetConcurrency = {}", resultSetConcurrency);

            callableStatement = connection.prepareCall(sql, resultSetType != null ? resultSetType : ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                       resultSetConcurrency != null ? resultSetConcurrency : ResultSet.CONCUR_READ_ONLY);

            if (fetchSize != null)
              callableStatement.setFetchSize(fetchSize);

            if (maxRows != null)
              callableStatement.setMaxRows(maxRows);
          
            if (cursorName != null)
              callableStatement.setCursorName(cursorName);
            
            if (escapeProcessing != null)
              callableStatement.setEscapeProcessing(escapeProcessing);

            if (maxFieldSize != null)
              callableStatement.setMaxFieldSize(maxFieldSize);
            
            if (queryTimeout != null)
              callableStatement.setQueryTimeout(queryTimeout);
            
            if (fetchDirection != null)
              callableStatement.setFetchDirection(fetchDirection);

            callableSql = sql;

            return (CallableStatement)callableStatement;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    PreparedStatement getPreparedStatementForQuery(String sql) throws DatabaseException
      {
        if (sql == null || sql.length() == 0)
          throw new DatabaseException(DatabaseException.SQL_STATEMENT_NULL);

        try
          {
            if (queryStatement != null && queryStatement instanceof PreparedStatement && sql.equals(querySql))
              return (PreparedStatement)queryStatement;

            logger.debug("Creating prepared statement for querying:");
            logger.debug("sql = {}", sql);
            logger.debug("resultSetType = {}", resultSetType);
            logger.debug("resultSetConcurrency = {}", resultSetConcurrency);

            queryStatement = connection.prepareStatement(sql, resultSetType != null ? resultSetType : ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                         resultSetConcurrency != null ? resultSetConcurrency : ResultSet.CONCUR_READ_ONLY);

            if (fetchSize != null)
              queryStatement.setFetchSize(fetchSize);

            if (maxRows != null)
              queryStatement.setMaxRows(maxRows);
          
            if (cursorName != null)
              queryStatement.setCursorName(cursorName);
            
            if (escapeProcessing != null)
              queryStatement.setEscapeProcessing(escapeProcessing);

            if (maxFieldSize != null)
              queryStatement.setMaxFieldSize(maxFieldSize);
            
            if (queryTimeout != null)
              queryStatement.setQueryTimeout(queryTimeout);
            
            if (fetchDirection != null)
              queryStatement.setFetchDirection(fetchDirection);

            querySql = sql;

            return (PreparedStatement)queryStatement;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    PreparedStatement getPreparedStatementForUpdate(String sql, List<String> keys) throws DatabaseException
      {
        if (sql == null || sql.length() == 0)
          throw new DatabaseException(DatabaseException.SQL_STATEMENT_NULL);

        logger.debug("Creating prepared statement for updating");

        try
          {
            if (updateStatement != null && updateStatement instanceof PreparedStatement && sql.equals(updateSql))
              return (PreparedStatement)updateStatement;
            else if (isBatch)
              {
                if ((updateStatement = batchStatements.get(sql)) != null)
                  {
                    updateSql = sql;
                    
                    return (PreparedStatement)updateStatement;
                  }
              }
            else closeUpdateStatement();

            if (keys != null && keys.size() > 0)
              updateStatement = getConnection().prepareStatement(sql, (String[])keys.toArray(new String[keys.size()]));
            else if (keys != null)
              updateStatement = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            else
              updateStatement = getConnection().prepareStatement(sql);

            updateSql = sql;

            if (isBatch)
              {
                batchStatements.put(updateSql, updateStatement);
                batchExecuteOrder.add(updateStatement);
              }
            
            return (PreparedStatement)updateStatement;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Returns the current statement object that is being used for queries.  The statement object can be either a java.sqlStatement or a java.sql.PreparedStatement
     * @return the current statement
     * @deprecated not useable
     */
    @Deprecated
    public Statement getQueryStatement() { return queryStatement; }
    
    /**
     * Returns the current statement object that is being used for updates.  The statement object can be either a java.sqlStatement or a java.sql.PreparedStatement
     * @return the current statement
     * @deprecated not useable
     */
    @Deprecated
    public Statement getUpdateStatement() { return updateStatement; }
    
    /**
     * Define whether or not to include associations.  This overrides the 
     * PersistentClassManager and DatabaseManager.PersistentClassManager version.
     *
     * @param ignoreAssociations ignore associations if true
     */

    public void setIgnoreAssociations(Boolean ignoreAssociations)
      {
        this.ignoreAssociations = ignoreAssociations;
      }

    /**
     * Return the value of ignore associations.  This overrides the 
     * PersistentClassManager and DatabaseManager.PersistentClassManager version.
     * 
     * @return returns the boolean value of ignore associations.
     */

    public Boolean getIgnoreAssociations() { return ignoreAssociations; }
    
    /**
     * Builds a select query from the object and executes it.  Any methods matching table columns will be returned, 
     * and any values set will be used to build the where clause.
     * 
     * @param object any Object (POJO, PersistentObject, etc.)
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public <T> Result<T> queryObject(T object) throws DatabaseException
      {
        return queryObject(object, null, (Object[])null);
      }
    
    /**
     * Builds a select query from a class that matches up to a table.  Any methods matching table
     * columns will be used to build the column list.  externalClauses can begin with a where
     * clause or anything after the where clause.
     *
     * @param cs any class that matches up to a table
     *
     * @return a Result instance
     *
     * @throws DatabaseException
     */

    public <T> Result<T> queryObject(Class<T> cs) throws DatabaseException
      {
        return queryObject(cs, null, (Object[])null);
      }

    /**
     * Builds a select query from the object and executes it.  Any methods matching table columns will be returned.
     * externalClauses can begin with a where clause or anything after the where clause.
     * 
     * @param object any Object (POJO, PersistentObject, etc.)
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public <T> Result<T> queryObject(T object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        try
          {
            Result<T> result = ORMSupport.queryObject(this, object, false, externalClauses, externalClausesParameters);
            
            resultsList.add(result);
            
            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Builds a select query from a class that matches up to a table.  Any methods matching table
     * columns will be used to build the column list.  externalClauses can begin with a where
     * clause or anything after the where clause.
     *
     * @param cs any class that matches up to a table
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return a Result instance
     *
     * @throws DatabaseException
     */

    public <T> Result<T> queryObject(Class<T> cs, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        try
          {
            Result<T> result = ORMSupport.queryObject(this, cs, false, externalClauses, externalClausesParameters);

            resultsList.add(result);

            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Builds a select query from an objects values, and then loads the
     * object with the result.
     * 
     * @param object the object to load
     *
     * @return returns the object passed in
     *
     * @throws DatabaseException
     */
    
    public <T> T loadObject(T object) throws DatabaseException
      {
        return loadObject(object, null, (Object[])null);
      }

    /**
     * Builds a select query from a class, and then loads an instance of the
     * class with the result.
     *
     * @param cs the class to base the load on
     *
     * @return returns the object passed in
     *
     * @throws DatabaseException
     */

    public <T> T loadObject(Class<T> cs) throws DatabaseException
      {
        return loadObject(cs, null, (Object[])null);
      }

    /**
     * Builds a select query from an objects values, and then loads the
     * object with the result.
     * 
     * @param object the object to load
     * @param externalClauses external clauses, which can begin with a where clause or any clause after the where clause.
     * @param externalClausesParameters the parameters to use with external clauses, can be null (1.5+ can use varargs)
     *
     * @return returns the object passed in
     *
     * @throws DatabaseException
     */

    public <T> T loadObject(T object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Result<T> result = queryObject(object, externalClauses, externalClausesParameters);
            
        try
          {
            if (result.hasNext())
              return result.next(object);
          }
        finally { result.close(); }
        
        return null;
      }

    /**
     * Builds a select query from a class, and then loads an instance of the
     * class with the result.
     *
     * @param cs the class to base the load on
     *
     * @return returns the object passed in
     *
     * @throws DatabaseException
     */

    public <T> T loadObject(Class<T> cs, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Result<T> result = queryObject(cs, externalClauses, externalClausesParameters);

        try
          {
            if (result.hasNext())
              return result.next();
          }
        finally { result.close(); }
        
        return null;
      }

    /**
     * Builds a select query from an objects values, and then loads a collection
     * with the result.
     * 
     * @param collection an instance of Collection
     * @param object the object to load
     *
     * @return the Collection that was passed in
     *
     * @throws DatabaseException
     */
    
    public <T> Collection<T> loadObjects(Collection<T> collection, T object) throws DatabaseException
      {
        return loadObjects(collection, object, null, (Object[])null);
      }
   
    /**
     * Builds a select query from an objects class, and then loads a collection
     * with the result.
     *
     * @param collection an instance of Collection
     * @param cs the class to base the load on
     *
     * @return the Collection that was passed in
     *
     * @throws DatabaseException
     */

    public <T> Collection<T> loadObjects(Collection<T> collection, Class<T> cs) throws DatabaseException
      {
        return loadObjects(collection, cs, null, (Object[])null);
      }

    /**
     * Builds a select query from an object, and then loads a collection
     * with the result.
     * 
     * @param collection an instance of Collection
     * @param object the object to load
     * @param externalClauses external clauses, which can begin with a where clause or any clause after the where clause.
     * @param externalClausesParameters the parameters to use with external clauses, can be null (1.5+ can use varargs)
     *
     * @return the Collection that was passed in
     *
     * @throws DatabaseException
     */
    
    public <T> Collection<T> loadObjects(Collection<T> collection, T object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Result<T> result = queryObject(object, externalClauses, externalClausesParameters);
        
        try
          {
            return result.loadObjects(collection, (Class<T>)object.getClass());
          }
        finally { result.close(); }
      }
    
    /**
     * Builds a select query from an objects class, and then loads a collection
     * with the result.
     *
     * @param collection an instance of Collection
     * @param cs the class to base the load on
     * @param externalClauses external clauses, which can begin with a where clause or any clause after the where clause.
     * @param externalClausesParameters the parameters to use with external clauses, can be null (1.5+ can use varargs)
     *
     * @return the Collection that was passed in
     *
     * @throws DatabaseException
     */

    public <T> Collection<T> loadObjects(Collection<T> collection, Class<T> cs, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Result<T> result = queryObject(cs, externalClauses, externalClausesParameters);
        
        try
          {
            return result.loadObjects(collection, cs);
          }
        finally { result.close(); }
      }

    /**
     * Loads an objects associations.
     * 
     * @param object the object whose associations are to be loaded
     * 
     * @throws ejp.DatabaseException
     */
    public void loadAssociations(Object object) throws DatabaseException
      {
        try
          {
            ORMSupport.loadAssociations(this, object, false);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Builds either an update or an insert depending on whether the object is persistent and was previously loaded, or not, respectively.
     * 
     * @param object any Object (POJO, PersistentObject, etc.)
     *
     * @return number of rows updated
     *
     * @throws DatabaseException
     */
    
    public int saveObject(Object object) throws DatabaseException
      {
        return saveObject(object, null, null, (Object[])null);
      }
    
    /**
     * Builds either an update or an insert depending on whether the object is persistent and was previously loaded, or not, respectively.
     * externalClauses can begin with a where clause or anything after the where clause.  
     * 
     * @param object any Object (POJO, PersistentObject, etc.)
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return number of rows updated
     *
     * @throws DatabaseException
     */
    
    public int saveObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        try
          {
            return ORMSupport.objectTransaction(this, object, ORMSupport.TRANS_SAVE_OBJECT, externalClauses, externalClausesParameters);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Builds either an insert.
     * 
     * @param object any Object
     *
     * @return number of rows updated
     *
     * @throws DatabaseException
     * @deprecated use saveObject
     */
    @Deprecated
    public int insertObject(Object object) throws DatabaseException
      {
        try
          {
            return saveObject(object);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Builds an update.
     * 
     * @param object any Object
     *
     * @return number of rows updated
     *
     * @throws DatabaseException
     * @deprecated use saveObject()
     */
    @Deprecated
    public int updateObject(Object object) throws DatabaseException
      {
        return updateObject(object, null, null, (Object[])null);
      }
    
    /**
     * Builds an update.
     * externalClauses can begin with a where clause or anything after the where clause.  
     * 
     * @param object any Object
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return number of rows updated
     *
     * @throws DatabaseException
     * @deprecated use saveObject()
     */
    @Deprecated
    public int updateObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        try
          {
            return saveObject(object, externalClauses, externalClausesParameters);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Builds a delete statement from the object.
     * 
     * @param object any Object
     *
     * @return number of rows deleted
     *
     * @throws DatabaseException
     */
    
    public int deleteObject(Object object) throws DatabaseException
      {
        return deleteObject(object, null, null, (Object[])null);
      }
    
    /**
     * Builds a delete statement from the object.  externalClauses can begin with a where clause or anything after 
     * the where clause.
     * 
     * @param object any Object
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return number of rows deleted
     *
     * @throws DatabaseException
     */
    
    public int deleteObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        try
          {
            return ORMSupport.objectTransaction(this, object, ORMSupport.TRANS_DELETE_OBJECT, externalClauses, externalClausesParameters);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Executes an SQL statement.  Returns a Result which may or may not currently point to 
     * a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * 
     * @param sql the SQL statement
     * @param keys is a List.  If keys is non-null, then generated keys will be returned in 
     *             the keys List.  List can also define the key columns required 
     *             (depending on the database).
     *
     * @return returns a Result which may or may not currently point to a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * @throws DatabaseException
     */
    
    public Result execute(String sql, List keys) throws DatabaseException
      {
        return execute(null, sql, keys);
      }
    
    /**
     * Executes an SQL statement.  Returns a Result which may or may not currently point to 
     * a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * 
     * @param cs is the class that will be associated with the result
     * @param sql the SQL statement
     * @param keys is a List.  If keys is non-null, then generated keys will be returned in 
     *             the keys List.  List can also define the key columns required 
     *             (depending on the database).
     *
     * @return returns a Result which may or may not currently point to a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * @throws DatabaseException
     */
    
    public <T> Result<T> execute(Class<T> cs, String sql, List keys) throws DatabaseException
      {
        logger.debug("sql = {}", sql);
        logger.debug("keys = {}", keys);

        try
          {
            boolean rval = false;
            Statement statement = getStatement();

            if (keys != null && keys.size() > 0)
              rval = statement.execute(sql, (String[])keys.toArray(new String[keys.size()]));
            else if (keys != null)
              rval = statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
            else
              rval = statement.execute(sql);

            if (keys != null)
              {
                ResultSet resultKeys = statement.getGeneratedKeys();

                keys.clear();

                while (resultKeys.next())
                  keys.add(new Long(resultKeys.getLong(1)));
              }

            logger.debug("keys = {}, rows updated = {}", keys, rval);

            Result<T> result = new Result(this, statement, statement.getResultSet(), !rval, cs);

            resultsList.add(result);

            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Executes a SQL query.
     * 
     * @param sql the SQL statement
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public Result executeQuery(String sql) throws DatabaseException
      {
        return executeQuery(null, sql);
      }
    
    /**
     * Executes a SQL query.
     * 
     * @param sql the SQL statement
     * @param cs is the class that will be associated with the result
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     * @deprecated parameters are in the wrong order
     */
    @Deprecated 
    public <T> Result<T> executeQuery(String sql, Class<T> cs) throws DatabaseException
      {
        return executeQuery(cs, sql);
      }
    
    /**
     * Executes a SQL query.
     * 
     * @param cs is the class that will be associated with the result
     * @param sql the SQL statement
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public <T> Result<T> executeQuery(Class<T> cs, String sql) throws DatabaseException
      {
        logger.debug(sql);

        if (sql == null || sql.length() == 0)
          throw new DatabaseException(DatabaseException.SQL_STATEMENT_NULL);

        try
          {
            Statement statement = getStatement();
            
            Result<T> result = new Result(this, statement.executeQuery(sql), cs);
            
            resultsList.add(result);
            
            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query uses a prepared statement and allows the use of '?' in 
     * SQL statements.
     * 
     * @param sql the SQL statement
     * @param parameters an array of objects used to set the parameters to the query
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public Result parameterizedQuery(String sql, Object... parameters) throws DatabaseException
      {
        return parameterizedQuery(null, sql, parameters);
      }
    
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query allows the use of '?' in 
     * SQL statements.
     * 
     * @param cs is the class that will be associated with the result
     * @param sql the SQL statement
     * @param parameters an array of objects used to set the parameters to the query
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     * @deprecated parameters are in the wrong order
     */
    @Deprecated 
    public <T> Result<T> parameterizedQuery(String sql, Class<T> cs, Object... parameters) throws DatabaseException
      {
        return parameterizedQuery(cs, sql, parameters);
      }
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query uses a prepared statement and allows the use of '?' in 
     * SQL statements.
     * 
     * @param cs is the class that will be associated with the result
     * @param sql the SQL statement
     * @param parameters an array of objects used to set the parameters to the query
     *
     * @return a ejp.Result instance 
     *
     * @throws DatabaseException
     */
    
    public <T> Result<T> parameterizedQuery(Class<T> cs, String sql, Object... parameters) throws DatabaseException
      {
        if (logger.isDebugEnabled())
          {
            logger.debug("sql = {}", sql);
            logger.debug("parameters[] = {}", StringUtils.toString(parameters));
          }

        try
          {
            PreparedStatement preparedStatement = getPreparedStatementForQuery(sql);

            setPreparedStatementObjects(preparedStatement, parameters);

            preparedStatement.execute();
            
            Result<T> result = new Result(this, preparedStatement.getResultSet(), cs);
            
            resultsList.add(result);
            
            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Executes a SQL update.
     * 
     * @param sql the SQL statement
     *
     * @throws DatabaseException
     */
    
    public int executeUpdate(String sql) throws DatabaseException
      {
        return executeUpdate(sql, (List<String>)null);
      }
    
    /**
     * Executes a SQL update.
     * 
     * @param sql the SQL statement
     * @param keys is a List.  If keys is non-null, then generated keys will be returned in 
     *             the keys List.  List can also define the key columns required 
     *             (depending on the database).
     *
     * @throws DatabaseException
     */
    
    public int executeUpdate(String sql, List keys) throws DatabaseException
      {
        logger.debug("sql = {}", sql);
        logger.debug("keys = {}", keys);

        try
          {
            int rval = 0;
            Statement statement = getStatementForUpdate();

            if (isBatch)
              statement.addBatch(sql);
            else
              {
                if (keys != null && keys.size() > 0)
                  rval = statement.executeUpdate(sql, (String[])keys.toArray(new String[keys.size()]));
                else if (keys != null)
                  rval = statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                else
                  rval = statement.executeUpdate(sql);

                if (keys != null)
                  {
                    ResultSet resultKeys = statement.getGeneratedKeys();

                    keys.clear();

                    while (resultKeys.next())
                      keys.add(new Long(resultKeys.getLong(1)));
                  }

                logger.debug("keys = {}, rows updated = {}", keys, rval);
              }

            return rval;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Executes a parameterized (prepared statement) update.  A parameterized update uses a prepared statement and allows the use of '?' in 
     * SQL statements.
     * 
     * @param sql the SQL statement
     * @param parameters an array of objects used to set the parameters to the update
     *
     * @throws DatabaseException
     */
    
    public int parameterizedUpdate(String sql, Object... parameters) throws DatabaseException
      {
        return parameterizedUpdate(sql, null, parameters);
      }
    
    /**
     * Executes a parameterized (prepared statement) update.  A parameterized update uses a prepared statement and allows the use of '?' in 
     * SQL statements.
     * 
     * @param sql the SQL statement
     * @param keys is a List.  If keys is non-null, then generated keys will be returned in 
     *             the keys List.  List can also define the key columns required 
     *             (depending on the database).
     * @param parameters an array of objects used to set the parameters to the update
     *
     * @throws DatabaseException
     */
    
    public int parameterizedUpdate(String sql, List keys, Object... parameters) throws DatabaseException
      {
        try
          {
            if (logger.isDebugEnabled())
              {
                logger.debug("sql = " + sql);
                logger.debug("parameters[] = " + StringUtils.toString(parameters));
                logger.debug("keys = " + keys);
              }

            int rval = 0;
            PreparedStatement preparedStatement = getPreparedStatementForUpdate(sql, keys);

            setPreparedStatementObjects(preparedStatement, parameters);

            if (isBatch)
              preparedStatement.addBatch();
            else
              {
                rval = preparedStatement.executeUpdate();

                if (keys != null)
                  {
                    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();

                    keys.clear();

                    while (generatedKeys.next())
                      keys.add(generatedKeys.getObject(1));
                  }

                logger.debug("keys = {}, rows updated = {}", keys, rval);
              }

            return rval;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Used to call stored procedures and functions.  Use the JDBC java.sql.CallableStatement syntax:
     * <pre>
     *      {?= call <procedure-name>[(?, ?, ?, ...)]}
     *      {call <procedure-name>[(?, ?, ?, ...)]}
     * </pre>
     * @param sql The JDBC stored procedure syntax needed to call your stored procedure.  See java.sql.CallableStatement.
     * @param parameters variable argument list of IN/OUT parameters. See InParameter, OutParameter, and InOutParameter
     * @return returns a Result which may or may not currently point to a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * @throws DatabaseException 
     * @see java.sql.CallableStatement
     */
    public Result storedProcedure(String sql, CallableParameter... parameters) throws DatabaseException
      {
        return storedProcedure(null, sql, parameters);
      }

    /**
     * Used to call stored procedures and functions.  Use the JDBC java.sql.CallableStatement syntax:
     * <pre>
     *      {?= call &lt;procedure-name&gt;[(?, ?, ?, ...)]}
     *      {call &lt;procedure-name&gt;[(?, ?, ?, ...)]}
     * </pre>
     * @param <T> Your class type
     * @param cs YourClass.class
     * @param sql The JDBC stored procedure syntax needed to call your stored procedure.  See java.sql.CallableStatement.
     * @param parameters variable argument list of IN/OUT parameters. See InParameter, OutParameter, and InOutParameter
     * @return returns a Result which may or may not currently point to a result set.  Use Result's isUpdateCount(), getUpdateCount(), and getMoreResults() to access more results.
     * @throws DatabaseException 
     * @see java.sql.CallableStatement
     */
    public <T> Result<T> storedProcedure(Class<T> cs, String sql, CallableParameter... parameters) throws DatabaseException
      {
        if (logger.isDebugEnabled())
          {
            logger.debug("sql = {}", sql);
            logger.debug("parameters[] = {}", StringUtils.toString(parameters));
          }

        try
          {
            CallableStatement statement = getCallableStatement(sql);

            setCallableStatementObjects(statement, parameters);

            boolean rval = statement.execute();
            
            Result<T> result = new Result(this, statement, statement.getResultSet(), !rval, cs);
            
            resultsList.add(result);
            
            return result;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Used to define IN/OUT parameters for stored procedures. Use InParameter, 
     * OutParameter, or InOutParameter
     */
    
    public static class CallableParameter
      {
        int position, sqlType, scale;
        Object value;
        
        CallableParameter() {}
      }
    
    /**
     * Used to define an IN parameter for a stored procedure.
     */
    
    public static class InParameter extends CallableParameter
      {
        public InParameter(int position, Object value)
          {
            this.position = position;
            this.value = value;
          }
      }    

    /**
     * Used to define an OUT parameter for a stored procedure.
     */
    
    public static class OutParameter extends CallableParameter
      {
        public OutParameter(int position, int sqlType)
          {
            this.position = position;
            this.sqlType = sqlType;
          }

        public OutParameter(int position, int sqlType, int scale)
          {
            this.position = position;
            this.sqlType = sqlType;
            this.scale = scale;
          }
      }
    
    /**
     * Used to define an IN/OUT parameter for a stored procedure.
     */
    
    public static class InOutParameter extends CallableParameter
      {
        public InOutParameter(int position, Object value, int sqlType)
          {
            this.position = position;
            this.value = value;
            this.sqlType = sqlType;
          }

        public InOutParameter(int position, Object value, int sqlType, int scale)
          {
            this.position = position;
            this.value = value;
            this.sqlType = sqlType;
            this.scale = scale;
          }
      }

    /**
     * If you are using JTA or some other transaction API and do not want JDBC 
     * level transaction support, then you will want to set 
     * setAutomaticTransactions() to false.  The default is true.
     * 
     * @param automaticTransactions true if using automatic JDBC transaction support.
     */
    public void setAutomaticTransactions(boolean usingExternalTransactions)
      {
        this.automaticTransactions = usingExternalTransactions;
      }
    
    /**
     * Returns the value of automaticTransactions.
     * 
     * @return true if using automatic JDBC transaction support.
     */
    public boolean getAutomaticTransactions() { return automaticTransactions; }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void setAutoCommit(boolean autoCommit) throws DatabaseException
      {
        logger.debug("Setting auto commit to {}", autoCommit);
        
        try
          {
            getConnection().setAutoCommit(autoCommit);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Starts a transaction by setting auto commit to false.  endTransaction() should always be called to reset the auto commit mode.
     */
    
    public void beginTransaction() throws DatabaseException
      {
        logger.debug("Starting transaction");
        
        setAutoCommit(false);
      }
    
    /**
     * Ends a transaction by setting auto commit to true (also commits the transaction).
     * Should always be called, even after commit or rollback.
     */
    
    public void endTransaction() throws DatabaseException
      {
        logger.debug("End transaction");
        
        setAutoCommit(true);
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public boolean getAutoCommit() throws DatabaseException
      {
        try
          {
            return getConnection().getAutoCommit();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void commit() throws DatabaseException
      {
        logger.debug("Commiting transaction");

        try
          {
            getConnection().commit();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void rollback() throws DatabaseException
      {
        logger.debug("Rolling back transaction");
        
        try
          {
            getConnection().rollback();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void rollback(Savepoint savepoint) throws DatabaseException
      {
        logger.debug("Rolling back transaction");
        
        try
          {
            getConnection().rollback(savepoint);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public Savepoint setSavepoint() throws DatabaseException
      {
        logger.debug("Adding savepoint");
        
        try
          {
            return getConnection().setSavepoint();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public Savepoint setSavepoint(String name) throws DatabaseException
      {
        logger.debug("Adding savepoint named = {}", name);
        
        try
          {
            return getConnection().setSavepoint(name);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void releaseSavepoint(Savepoint savepoint) throws DatabaseException
      {
        logger.debug("Releasing savepoint");
        
        try
          {
            if (isClosed)
              throw new DatabaseException(DatabaseException.DATABASE_CLOSED);

            getConnection().releaseSavepoint(savepoint);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Start adding sql statements to batch update processing.  If the statement 
     * type (Statement, PreparedStatement) changes or a PreparedStatements SQL 
     * changes the current batch is executed and a new batch is started.
     *
     * Please note that objects are not persisted during/following batch saves
     * (inserts/updates), as the inserts/updates take place long after
     * the saveObject() method via the executeBatch() method, end there
     * is no way at that point to get auto-generated column information
     * and/or to reload the object with the database.  EJP simply uses
     * the information from the objects during the saveObject() method
     * call to build the batch.
     */
    public void beginBatch() throws DatabaseException
      {
        if (getMetaData().supportsBatchUpdates())
          {
            isBatch = true;
            
            try
              {
                closeUpdateStatement();
                batchStatements = new HashMap<String, Statement>();
                batchExecuteOrder = new ArrayList<Statement>();
                
              }
            catch (Exception e)
              {
                throw new DatabaseException(e);
              }
            
            batchUpdateCounts = new ArrayList<Integer>();
          }
      }

    /**
     * Executes the statements that have been added to batch processing.
     * 
     * Please note that objects are not persisted during/following batch saves
     * (inserts/updates), as the inserts/updates take place long after
     * the saveObject() method via the executeBatch() method, end there
     * is no way at that point to get auto-generated column information
     * and/or to reload the object with the database.  EJP simply uses
     * the information from the objects during the saveObject() method
     * call to build the batch.
     *
     * @throws ejp.DatabaseException
     */
    public void executeBatch() throws DatabaseException
      {
        if (isBatch)
          try
            {
              for (Statement statement : batchExecuteOrder)
                for (Integer count : statement.executeBatch())
                  batchUpdateCounts.add(count);
            }
          catch (Exception e)
            {
              throw new DatabaseException(e);
            }
      }

    /**
     * Returns the update counts for any previous batch updates (between beginBatch() and endBatch()).
     * 
     * @return an array of Integer values representing the update counts
     */
    public Integer[] getBatchUpdateCounts() 
      {
        Integer[] updateCounts = null;
        
        if (batchUpdateCounts != null)
          updateCounts = (Integer[]) batchUpdateCounts.toArray(new Integer[batchUpdateCounts.size()]); 
        
        return updateCounts;
      }

    /**
     * Clears the batch update counts.
     */
    public void clearBatchUpdateCounts() 
      {
        if (batchUpdateCounts != null)
          batchUpdateCounts.clear(); 
      }

    /**
     * Ends batch update processing.
     */
    public void endBatch() throws DatabaseException
      {
        if (isBatch)
          {
            try
              {
                if (batchStatements != null)
                  {
                    for (Statement statement : batchExecuteOrder)
                      statement.close();

                    batchStatements.clear();
                    batchExecuteOrder.clear();
                  }

                batchStatements = null;
                batchExecuteOrder = null;
                updateStatement = null;
              }
            catch (Exception e)
              {
                throw new DatabaseException(e);
              }

            clearBatchUpdateCounts();
            batchUpdateCounts = null;
            isBatch = false;
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public SQLWarning getWarnings() throws DatabaseException
      {
        try
          {
            return getConnection().getWarnings();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Connection
     *
     * @see java.sql.Connection
     */
    
    public void clearWarnings() throws DatabaseException
      {
        try
          {
            getConnection().clearWarnings();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    static void setPreparedStatementObjects(PreparedStatement preparedStatement, Object... objects) throws DatabaseException
      {
        try
          {
            for (int i = 0; i < objects.length; i++)
              {
                if (objects[i] instanceof NullValue)
                  preparedStatement.setNull(i+1, ((NullValue)objects[i]).getSqlType());
                else if (objects[i] instanceof Array)
                  preparedStatement.setArray(i+1, (Array)objects[i]);
                else if (objects[i] instanceof BigDecimal)
                  preparedStatement.setBigDecimal(i+1, (BigDecimal)objects[i]);
                else if (objects[i] instanceof Blob)
                  preparedStatement.setBlob(i+1, (Blob)objects[i]);
                else if (objects[i] instanceof Clob)
                  preparedStatement.setClob(i+1, (Clob)objects[i]);
                else if (objects[i] instanceof Date)
                  preparedStatement.setDate(i+1, (Date)objects[i]);
                else if (objects[i] instanceof InputStream)
                  {
                    throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
                  }
                else if (objects[i] instanceof AsciiStream)
                  preparedStatement.setAsciiStream(i+1, ((AsciiStream)objects[i]).getInputStream(), ((AsciiStream)objects[i]).getLength());
                else if (objects[i] instanceof BinaryStream)
                  preparedStatement.setBinaryStream(i+1, ((BinaryStream)objects[i]).getInputStream(), ((BinaryStream)objects[i]).getLength());
                else if (objects[i] instanceof Reader)
                  {
                    throw new DatabaseException("Must use ejp.interfaces.CharacterStream instead of Reader");
                  }
                else if (objects[i] instanceof CharacterStream)
                  preparedStatement.setCharacterStream(i+1, ((CharacterStream)objects[i]).getReader(), ((CharacterStream)objects[i]).getLength());
                else if (objects[i] instanceof Ref)
                  preparedStatement.setRef(i+1, (Ref)objects[i]);
                else if (objects[i] instanceof String)
                  preparedStatement.setString(i+1, (String)objects[i]);
                else if (objects[i] instanceof Time)
                  preparedStatement.setTime(i+1, (Time)objects[i]);
                else if (objects[i] instanceof Timestamp)
                  preparedStatement.setTimestamp(i+1, (Timestamp)objects[i]);
                else if (objects[i] instanceof URL)
                  preparedStatement.setURL(i+1, (URL)objects[i]);
                else
                  preparedStatement.setObject(i+1, objects[i]);
              }
          }
        catch (SQLException e)
          {
            throw new DatabaseException(e);
          }
      }

    static void setCallableStatementObjects(CallableStatement callableStatement, CallableParameter... parameters) throws DatabaseException
      {
        try
          {
            for (CallableParameter callableParameter : parameters)
              {
                if (callableParameter instanceof OutParameter || callableParameter instanceof InOutParameter)
                  {
                    callableStatement.registerOutParameter(callableParameter.position, callableParameter.sqlType, callableParameter.scale);
                  }

                if (callableParameter instanceof InParameter || callableParameter instanceof InOutParameter)
                  {
                    int position = callableParameter.position;
                    Object value = callableParameter.value;
                  
                    if (value instanceof NullValue)
                      callableStatement.setNull(position, ((NullValue)value).getSqlType());
                    else if (value instanceof Array)
                      callableStatement.setArray(position, (Array)value);
                    else if (value instanceof BigDecimal)
                      callableStatement.setBigDecimal(position, (BigDecimal)value);
                    else if (value instanceof Blob)
                      callableStatement.setBlob(position, (Blob)value);
                    else if (value instanceof Clob)
                      callableStatement.setClob(position, (Clob)value);
                    else if (value instanceof Date)
                      callableStatement.setDate(position, (Date)value);
                    else if (value instanceof InputStream)
                      {
                        throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
                      }
                    else if (value instanceof AsciiStream)
                      callableStatement.setAsciiStream(position, ((AsciiStream)value).getInputStream(), ((AsciiStream)value).getLength());
                    else if (value instanceof BinaryStream)
                      callableStatement.setBinaryStream(position, ((BinaryStream)value).getInputStream(), ((BinaryStream)value).getLength());
                    else if (value instanceof Reader)
                      {
                        throw new DatabaseException("Must use ejp.interfaces.CharacterStream instead of Reader");
                      }
                    else if (value instanceof CharacterStream)
                      callableStatement.setCharacterStream(position, ((CharacterStream)value).getReader(), ((CharacterStream)value).getLength());
                    else if (value instanceof Ref)
                      callableStatement.setRef(position, (Ref)value);
                    else if (value instanceof String)
                      callableStatement.setString(position, (String)value);
                    else if (value instanceof Time)
                      callableStatement.setTime(position, (Time)value);
                    else if (value instanceof Timestamp)
                      callableStatement.setTimestamp(position, (Timestamp)value);
                    else if (value instanceof URL)
                      callableStatement.setURL(position, (URL)value);
                    else
                      callableStatement.setObject(position, value);
                  }
              }
          }
        catch (SQLException e)
          {
            throw new DatabaseException(e);
          }
      }
  }

