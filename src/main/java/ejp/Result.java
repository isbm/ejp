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
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ejp.interfaces.AsciiStream;
import ejp.interfaces.AsciiStreamAdapter;
import ejp.interfaces.BinaryStream;
import ejp.interfaces.BinaryStreamAdapter;
import ejp.interfaces.CharacterStreamAdapter;
import ejp.interfaces.CharacterStream;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

/**
 * The result class is created and returned by all query methods in ejp.Database,
 * and basically wraps a ResultSet with ResultSet
 * functionality, along with ListIterator and object mapping functionality.
 */
@SuppressWarnings("unchecked")
public class Result<T> implements ListIterator<T>, Iterable<T>
  {
    private static Logger logger = LoggerFactory.getLogger(Result.class);
    private Map<String,Integer> columnHash;
    private boolean isClosed, lastHas;
    private Boolean ignoreAssociations, isUpdateCount;
    private ResultSet resultSet;
    private Statement statement;
    private Database db;
    private Class<T> cs;
    
    /**
     * Create an instance of Result by passing in a ejp.Database instance 
     * and a ResultSet instance returned from a query (statement.resultSet).
     *
     * @param db 
     * @param resultSet
     */
    public Result(Database db, ResultSet resultSet) throws DatabaseException
      {
        this.db = db;
        this.resultSet = resultSet;
        
        try
          {
            this.statement = resultSet.getStatement();
            initColumnHash();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Create an instance of Result by passing in a ejp.Database instance 
     * and a ResultSet instance returned from a query (statement.resultSet).
     *
     * @param db 
     * @param resultSet
     *
     * Setting the class allows next() and previous() to return loaded 
     * (with data from the current row) objects of the class.
     */
    public Result(Database db, ResultSet resultSet, Class<T> cs) throws DatabaseException
      {
        this.db = db;
        this.resultSet = resultSet;
        this.cs = cs;
        
        try
          {
            this.statement = resultSet.getStatement();
            initColumnHash();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Create an instance of Result by passing in a ejp.Database instance 
     * and a ResultSet instance returned from a query (statement.resultSet).
     *
     * @param db 
     * @param resultSet
     *
     * Setting the class allows next() and previous() to return loaded 
     * (with data from the current row) objects of the class.
     */
    public Result(Database db, Statement statement, ResultSet resultSet, Boolean isUpdateCount, Class<T> cs) throws DatabaseException
      {
        this.db = db;
        this.statement = statement;
        this.resultSet = resultSet;
        this.isUpdateCount = isUpdateCount;
        this.cs = cs;
        
        try
          {
            if (resultSet != null)
              initColumnHash();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    final void initColumnHash()
      {
        try
          {
            ResultSetMetaData m = resultSet.getMetaData();
            columnHash = new HashMap<String, Integer>();

            for (int i = 0; i < m.getColumnCount(); i++)
              {
                columnHash.put(m.getColumnLabel(i+1).toLowerCase(), i + 1);
                columnHash.put(m.getColumnName(i+1).toLowerCase(), i + 1);
              }
          } catch (Exception e) { logger.debug(e.toString(), e); }
      }
    
    /**
     * Returns the column number for the column associated with columnName.
     * 
     * @param columnName the column name 
     * @return the column number for column name
     */
    public Integer getColumnNumber(String columnName)
      {
        if (columnHash != null)
          return columnHash.get(columnName);
        
        return null;
      }
    
    /**
     * If class (see setClass()) is defined, iteration will return instances of 
     * class loaded with the current row of data.
     *
     * @return returns this
     */
    
    public Iterator<T> iterator() { return this; }

    /**
     * If class (see setClass()) is defined, iteration will return instances of 
     * class loaded with the current row of data.
     *
     * @return returns this
     */
    
    public ListIterator<T> listIterator() { return this; }

    /**
     * Setting the class allows next() and previous() to return loaded 
     * (with data from the current row) objects of the class. 
     * Results returned by ejp.Database and ejp.DatabaseManager queries with 
     * YourObject.class in them automatically set the class.  
     * 
     * This is the same as getResultSetWithClass.
     *
     * @param cs the class to create instances for
     *
     * @return returns this
     */
    public Result<T> setClass(Class<T> cs)
      {
        this.cs = cs;
        
        return (Result<T>)this;
      }
    
    /**
     * Setting the class allows next() and previous() to return loaded 
     * (with data from the current row) objects of the class. 
     * Results returned by ejp.Database and ejp.DatabaseManager queries with 
     * YourObject.class in them automatically set the class.
     *
     * This is the same as setClass.
     * 
     * @param cs the class to create instances for
     *
     * @return returns this
     */
    public Result<T> getResultSetWithClass(Class<T> cs)
      {
        this.cs = cs;
        
        return (Result<T>)this;
      }
    
    /**
     * Closes all resources associated with a JDBC Statement and ResultSet.
     */
    public void close()// throws DatabaseException
      {
        if (!isClosed)
          {
            db.closeResult(this);
            isClosed = true;
          }
      }
    
    /**
     * Closes the result (simply calls close()), can be used in EL. For example:
     * 
     * <pre>
     *     ${result.close}
     * </pre>
     *
     * @throws DatabaseException
     */
    
    public String getClose()// throws DatabaseException
      {
        close();
        
        return null;
      }
            
    /**
     * Closes the result (simply calls close()), via a JavaBeans setter.  For example:
     * <pre>
     *     <jsp:useBean id="result" scope="request" class="ejp.Result" />
     *     <jsp:setProperty name="result" property="closed" value="true"/>
     * </pre>
     * 
     * @throws DatabaseException
     */
    
    public void setClosed(boolean true_only)// throws DatabaseException
      {
        close();
      }
            
    /**
     * Returns true if the result is closed, false otherwise.
     *
     * @return true or false
     */
    
    public boolean isClosed() { return isClosed; }
    
    /**
     * Return the current ResultSet, if there is a current ResultSet instance.
     * 
     * @return a JDBC ResultSet instance or null if there isn't one.
     */
    
    public ResultSet getResultSet()
      {
        return resultSet;
      }
    
    /**
     * Return the current ResultSet, if there is a current ResultSet instance.
     * 
     * @return a JDBC ResultSet instance or null if there isn't one.
     */
    
    public Database getDatabase()// throws DatabaseException
      {
        return db;
      }

    /**
     * Returns the current statement or null if no statement is active.
     * 
     * @return a JDBC statement
     */
    
    public Statement getStatement()// throws DatabaseException
      {
        return statement;
      }

    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public boolean getMoreResults() throws DatabaseException
      {
        try
          {
            if (statement != null && statement.getMoreResults())
              {
                isUpdateCount = false;
                resultSet = statement.getResultSet();
                initColumnHash();
                
                return true;
              }
            
            resultSet = null;
            isUpdateCount = true;

            return false;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public boolean getMoreResults(int doWhatWithCurrent) throws DatabaseException
      {
        try
          {
            if (statement != null && statement.getMoreResults(doWhatWithCurrent))
              {
                isUpdateCount = false;
                resultSet = statement.getResultSet();
                initColumnHash();
                
                return true;
              }
            
            resultSet = null;
            isUpdateCount = true;

            return false;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * True if the current result is an update count.
     *
     * @see java.sql.Statement
     */
    
    public boolean isUpdateCount() { return isUpdateCount; }

    /**
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public int getUpdateCount() throws DatabaseException
      {
        try
          {
            if (statement != null)
              return statement.getUpdateCount();
            
            return -1;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Load an instance of the class (using set methods that match columns in a table matched to the class name) 
     * with the results of the current ResultSet row.  This can be used to load any class at anytime.  
     * If class is defined then next() and previous() will load your objects automatically.  If class 
     * is not defined, then loadObject() can be used to fill your object(s) with data from the current row.
     * 
     * @param cs any class with an empty constructor
     *
     * @return a new instance of cs 
     *
     * @throws DatabaseException
     */
    
    public <C> C loadObject(Class<C> cs) throws DatabaseException
      {
        try
          {
            return loadObject(cs.newInstance());
          }
        catch (Exception ex)
          {
            throw new DatabaseException(ex);
          }
      }
    
    /**
     * Load the object (using set methods that match columns in a table matched to the class name) 
     * with the results of the current ResultSet row.  This can be used to load any class at anytime.  
     * If class is defined then next() and previous() will load your objects automatically.  If class 
     * is not defined, then loadObject() can be used to fill your object(s) with data from the current row.
     * 
     * @param object any Object (POJO, PersistentObject, etc.)
     *
     * @return the object passed in
     *
     * @throws DatabaseException
     */

    public <C> C loadObject(C object) throws DatabaseException
      {
        try
          {
            return (C)ORMSupport.loadObject(this, object);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Loads objects (using set methods that match columns in a table matched to the class name) 
     * into a collection with the results (all rows) of the current ResultSet.
     * 
     * @param collection an instance of Collection
     * @param cs any class with an empty constructor
     *
     * @return the collection passed in
     *
     * @throws DatabaseException
     */
    
    public <C> Collection<C> loadObjects(Collection<C> collection, Class<C> cs) throws DatabaseException
      {
        try
          {
            while (hasNext())
              collection.add((C)next(cs.newInstance()));
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
        
        return collection;
      }
    
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
            ORMSupport.loadAssociations(db, object, false);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void moveToCurrentRow() throws DatabaseException
      {
        try
          {
            resultSet.moveToCurrentRow();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void moveToInsertRow() throws DatabaseException
      {
        try
          {
            if (resultSet != null)
              resultSet.moveToInsertRow();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void insertRow() throws DatabaseException
      {
        try
          {
            resultSet.insertRow();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean rowInserted() throws DatabaseException
      {
        try
          {
            return resultSet.rowInserted();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void deleteRow() throws DatabaseException
      {
        try
          {
            resultSet.deleteRow(); 
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean rowDeleted() throws DatabaseException
      {
        try
          {
            return resultSet.rowDeleted();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void updateRow() throws DatabaseException
      {
        try
          {
            resultSet.updateRow();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean rowUpdated() throws DatabaseException
      {
        try
          {
            return resultSet.rowUpdated();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void setFetchDirection(int direction) throws DatabaseException
      {
        logger.debug("Setting fetch direction = {}", direction);
        
        try
          {
            resultSet.setFetchDirection(direction);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean first() throws DatabaseException
      {
        try
          {
            return resultSet.first();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean isFirst() throws DatabaseException
      {
        try
          {
            return resultSet.isFirst();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void beforeFirst() throws DatabaseException
      {
        try
          {
            resultSet.beforeFirst();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean isBeforeFirst() throws DatabaseException
      {
        try
          {
            return resultSet.isBeforeFirst();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean last() throws DatabaseException
      {
        try
          {
            return resultSet.last();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean isLast() throws DatabaseException
      {
        try
          {
            return resultSet.isLast();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void afterLast() throws DatabaseException
      {
        try
          {
            resultSet.afterLast();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public boolean isAfterLast() throws DatabaseException
      {
        try
          {
            return resultSet.isAfterLast();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ResultSet
     *
     * @see java.sql.ResultSet
     */
    
    public void refreshRow() throws DatabaseException
      {
        try
          {
            resultSet.refreshRow();
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * See same in java.sql.ListIterator
     *
     * @see java.util.ListIterator
     */
    
    public boolean hasNext()
      {
        try
          {
            if (lastHas)
              return true;
            
            return lastHas = resultSet.next();
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }

    /**
     * Returns the next record.  If object is non-null, it is loaded with 
     * matching data.  If the object is null and if class is defined, a new 
     * instance will be loaded and returned.  If object is null and class is not 
     * defined, then this (Result) is returned.
     *
     * @return the loaded object
     *
     * @see java.util.ListIterator
     */
    
    public T next()
      {
        try
          {
            return next(cs != null ? cs.newInstance() : null);
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }
    
    /**
     * Returns the next record.  If object is non-null, it is loaded with 
     * matching data.  If the object is null and if class is defined, a new 
     * instance will be loaded and returned.  If object is null and class is not 
     * defined, then this (Result) is returned.
     *
     * @param object the object to load
     * 
     * @return the loaded object
     * 
     * @see java.util.ListIterator
     */
    
    public <C> C next(C object)
      {
        try
          {
            if (lastHas || resultSet.next())
              {
                lastHas = false;
                
                if (object == null)
                  return (C)this;
                else
                  return loadObject(object);
              }
            
            return null;
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }

    /**
     * See same in java.sql.ListIterator
     *
     * @see java.util.ListIterator
     */
    
    public boolean hasPrevious() 
      {
        try
          {
            if (lastHas)
              return true;
            
            return lastHas = resultSet.previous();
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }
    
    /**
     * Returns the previous record.  If class is defined, a new 
     * instance will be loaded and returned.  If class is 
     * not defined, then this (Result) is returned.
     * 
     * @return the loaded object
     *
     * @see java.util.ListIterator
     */
     
    public T previous() 
      {
        try
          {
            return previous(cs != null ? cs.newInstance() : null);
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }
    
    /**
     * Returns the previous record.  If object is non-null, it is loaded with 
     * matching data.  If the object is null and if class is defined, a new 
     * instance will be loaded and returned.  If the object is null and class is 
     * not defined, then this (Result) is returned.
     *
     * @param object the object to load
     * 
     * @return the loaded object
     * 
     * @see java.util.ListIterator
     */
     
    public <C> C previous(C object) 
      {
        try
          {
            if (lastHas || resultSet.previous())
              {
                lastHas = false;
                
                if (object == null)
                  return (C)this;
                else
                  return loadObject(object);
              }
            
            return null;
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }

    /**
     * Returns the current record.  If class is defined, a new 
     * instance will be loaded and returned.  Otherwise, if class is 
     * not defined, then this (Result) is returned.  
     * 
     * @return the loaded object
     *
     * @see java.util.ListIterator
     */
     
    public T current()
      {
        try
          {
            return current(cs != null ? cs.newInstance() : null);
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }

    /**
     * Returns the current record.  If object is non-null, it is loaded with 
     * matching data.  If the object is null, then if class is defined, a new 
     * instance will be loaded and returned.  If the object is null and class is 
     * not defined, then this (Result) is returned.
     *
     * @param object the object to load
     * 
     * @return the loaded object
     * 
     * @see java.util.ListIterator
     */
     
    public <C> C current(C object)
      {
        try
          {
            return object == null ? (C)this : loadObject(object);
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }

    /**
     * See same in java.sql.ListIterator
     *
     * @see java.util.ListIterator
     */
    
    public int nextIndex() 
      {
        try
          {
            if (hasNext())
              return resultSet.getRow() + 1;
          }
        catch (Exception ex)
          {
            throw new RuntimeException(ex);
          }
        
        return -1;
      }
    
    /**
     * See same in java.sql.ListIterator
     *
     * @see java.util.ListIterator
     */
    
    public int previousIndex() 
      {
        try
          {
            if (hasPrevious())
              return resultSet.getRow() - 1;
          }
        catch (Exception ex)
          {
            throw new RuntimeException(ex);
          }
        
        return -1;
      }
    
    /**
     * See same in java.sql.ListIterator
     *
     * @see java.util.ListIterator
     */
    
    public void remove() 
      {
        try
          {
            deleteRow();
          }
        catch (Exception e)
          {
            throw new RuntimeException(e);
          }
      }
    
    /**
     * See same in java.sql.ListIterator.  Not supported, and no need to.
     *
     * @throws UnsupportedOperationException
     */
    
    public void add(Object o)  { throw new UnsupportedOperationException(); }
    
    /**
     * See same in java.sql.ListIterator.  Not supported, and no need to.
     *
     * @throws UnsupportedOperationException
     */
    
    public void set(Object o) { throw new UnsupportedOperationException(); }

    /**
     * Returns the object defined by named column from the current row in the result set.
     * 
     * @param columnName the column name
     *
     * @return the object
     *
     * @throws DatabaseException
     */
    
    public <C> C getColumnValue(String columnName) throws DatabaseException
      {
        try
          {
            return (C)resultSet.getObject(columnName); 
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Returns the object (after converting it to returnType) defined by named column from the current row in the result set.
     *
     * @param returnType converts the return type to returnType 
     * @param columnName the column name
     *
     * @return the object
     *
     * @throws DatabaseException
     */
    
    public <C> C getColumnValue(Class<C> returnType, String columnName) throws DatabaseException
      {
        try
          {
            Object value = null;

            if (returnType != null)
              if (returnType == Array.class)
                value = resultSet.getArray(columnName);
              else if (returnType == BigDecimal.class)
                value = resultSet.getBigDecimal(columnName);
              else if (returnType == Blob.class)
                value = resultSet.getBlob(columnName);
              else if (returnType == Clob.class)
                value = resultSet.getClob(columnName);
              else if (returnType == Date.class)
                value = resultSet.getDate(columnName);
              else if (returnType == InputStream.class)
                {
                  throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
                }          
              else if (returnType.isAssignableFrom(AsciiStream.class))
                value = new AsciiStreamAdapter(-1, resultSet.getAsciiStream(columnName));
              else if (returnType.isAssignableFrom(BinaryStream.class))
                value = new BinaryStreamAdapter(-1, resultSet.getBinaryStream(columnName));
              else if (returnType == Reader.class)
                {
                  throw new DatabaseException("Must use ejp.interfaces.ReaderHandler instead of Reader");
                }
              else if (returnType.isAssignableFrom(CharacterStream.class))
                value = new CharacterStreamAdapter(-1, resultSet.getCharacterStream(columnName));
              else if (returnType == Ref.class)
                value = resultSet.getRef(columnName);
              else if (returnType == String.class)
                value = resultSet.getString(columnName);
              else if (returnType == Time.class)
                value = resultSet.getTime(columnName);
              else if (returnType == Timestamp.class)
                value = resultSet.getTimestamp(columnName);
              else if (returnType == URL.class)
                value = resultSet.getURL(columnName);

            if (value == null)
              value = resultSet.getObject(columnName);

            return (C)value;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
     * Returns the object defined by column index from the current row in the result set.
     * 
     * @param columnIndex the column index
     *
     * @return the object
     *
     * @throws DatabaseException
     */
    
    public <C> C getColumnValue(int columnIndex) throws DatabaseException
      {
        try
          {
            return (C)resultSet.getObject(columnIndex); 
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Returns the object (after converting it to returnType) defined by named column from the current row in the result set.
     *
     * @param returnType converts the return type to returnType 
     * @param columnIndex the column index
     *
     * @return the object
     *
     * @throws DatabaseException
     */
    
    public <C> C getColumnValue(Class<C> returnType, int columnIndex) throws DatabaseException
      {
        try
          {
            Object value = null;

            if (returnType != null)
              if (returnType == Array.class)
                value = resultSet.getArray(columnIndex);
              else if (returnType == BigDecimal.class)
                value = resultSet.getBigDecimal(columnIndex);
              else if (returnType == Blob.class)
                value = resultSet.getBlob(columnIndex);
              else if (returnType == Clob.class)
                value = resultSet.getClob(columnIndex);
              else if (returnType == Date.class)
                value = resultSet.getDate(columnIndex);
              else if (returnType == InputStream.class)
                {
                  throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
                }          
              else if (returnType.isAssignableFrom(AsciiStream.class))
                value = new AsciiStreamAdapter(-1, resultSet.getAsciiStream(columnIndex));
              else if (returnType.isAssignableFrom(BinaryStream.class))
                value = new BinaryStreamAdapter(-1, resultSet.getBinaryStream(columnIndex));
              else if (returnType == Reader.class)
                {
                  throw new DatabaseException("Must use ejp.interfaces.ReaderHandler instead of Reader");
                }
              else if (returnType.isAssignableFrom(CharacterStream.class))
                value = new CharacterStreamAdapter(-1, resultSet.getCharacterStream(columnIndex));
              else if (returnType == Ref.class)
                value = resultSet.getRef(columnIndex);
              else if (returnType == String.class)
                value = resultSet.getString(columnIndex);
              else if (returnType == Time.class)
                value = resultSet.getTime(columnIndex);
              else if (returnType == Timestamp.class)
                value = resultSet.getTimestamp(columnIndex);
              else if (returnType == URL.class)
                value = resultSet.getURL(columnIndex);

            if (value == null)
              value = resultSet.getObject(columnIndex);

            return (C)value;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
    
    /**
     * Sets the object value defined by column name in the current row in the result set.
     * 
     * @param columnName the column name
     * @param object the object being stored in the database
     *
     * @throws DatabaseException
     */
    
    public void setColumnValue(String columnName, Object object) throws DatabaseException
      {
        try
          {
            if (object instanceof Array)
              resultSet.updateArray(columnName, (Array)object);
            else if (object instanceof BigDecimal)
              resultSet.updateBigDecimal(columnName, (BigDecimal)object);
            else if (object instanceof Blob)
              resultSet.updateBlob(columnName, (Blob)object);
            else if (object instanceof Clob)
              resultSet.updateClob(columnName, (Clob)object);
            else if (object instanceof Date)
              resultSet.updateDate(columnName, (Date)object);
            else if (object instanceof InputStream)
              {
                throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
              }
            else if (object instanceof AsciiStream)
              resultSet.updateAsciiStream(columnName, ((AsciiStream)object).getInputStream(), ((AsciiStream)object).getLength());
            else if (object instanceof BinaryStream)
              resultSet.updateBinaryStream(columnName, ((BinaryStream)object).getInputStream(), ((BinaryStream)object).getLength());
            else if (object instanceof Reader)
              {
                throw new DatabaseException("Must use ejp.interfaces.ReaderHandler instead of Reader");
              }
            else if (object instanceof CharacterStream)
              resultSet.updateCharacterStream(columnName, ((CharacterStream)object).getReader(), ((CharacterStream)object).getLength());
            else if (object instanceof Ref)
              resultSet.updateRef(columnName, (Ref)object);
            else if (object instanceof String)
              resultSet.updateString(columnName, (String)object);
            else if (object instanceof Time)
              resultSet.updateTime(columnName, (Time)object);
            else if (object instanceof Timestamp)
              resultSet.updateTimestamp(columnName, (Timestamp)object);
            //else if (object instanceof URL)
            //  resultSet.updateURL(columnName, (URL)object);
            else
              resultSet.updateObject(columnName, object);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    /**
   * Sets the object value defined by column index in the current row in the result set.
   * 
   * @param columnIndex the column number
   * @param object the object being stored in the database
   * @throws DatabaseException
   */
    
    public void setColumnValue(int columnIndex, Object object) throws DatabaseException
      {
        try
          {
            if (object instanceof Array)
              resultSet.updateArray(columnIndex, (Array)object);
            else if (object instanceof BigDecimal)
              resultSet.updateBigDecimal(columnIndex, (BigDecimal)object);
            else if (object instanceof Blob)
              resultSet.updateBlob(columnIndex, (Blob)object);
            else if (object instanceof Clob)
              resultSet.updateClob(columnIndex, (Clob)object);
            else if (object instanceof Date)
              resultSet.updateDate(columnIndex, (Date)object);
            else if (object instanceof InputStream)
              {
                throw new DatabaseException("Must use ejp.interfaces.AsciiStream or ejp.interfaces.BinaryStream instead of InputStream");
              }
            else if (object instanceof AsciiStream)
              resultSet.updateAsciiStream(columnIndex, ((AsciiStream)object).getInputStream(), ((AsciiStream)object).getLength());
            else if (object instanceof BinaryStream)
              resultSet.updateBinaryStream(columnIndex, ((BinaryStream)object).getInputStream(), ((BinaryStream)object).getLength());
            else if (object instanceof Reader)
              {
                throw new DatabaseException("Must use ejp.interfaces.ReaderHandler instead of Reader");
              }
            else if (object instanceof CharacterStream)
              resultSet.updateCharacterStream(columnIndex, ((CharacterStream)object).getReader(), ((CharacterStream)object).getLength());
            else if (object instanceof Ref)
              resultSet.updateRef(columnIndex, (Ref)object);
            else if (object instanceof String)
              resultSet.updateString(columnIndex, (String)object);
            else if (object instanceof Time)
              resultSet.updateTime(columnIndex, (Time)object);
            else if (object instanceof Timestamp)
              resultSet.updateTimestamp(columnIndex, (Timestamp)object);
            //else if (object instanceof URL)
            //  resultSet.updateURL(columnIndex, (URL)object);
            else
              resultSet.updateObject(columnIndex, object);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }
  }