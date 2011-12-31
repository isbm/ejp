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

import ejp.MetaData.Table;
import ejp.PersistentClassManager.ClassInformation;
import ejp.utilities.ObjectFiller;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ejp.utilities.ObjectConverter;
import ejp.utilities.ObjectFiller.GetHandler;
import ejp.utilities.ObjectFiller.ItemNotFoundException;
import ejp.utilities.StringUtils;
import ejp.interfaces.GeneratedKeys;
import ejp.annotations.GlobalDelete;
import ejp.annotations.GlobalUpdate;
import ejp.annotations.SingleTableInheritance;
import ejp.annotations.ConcreteTableInheritance;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

@SuppressWarnings("unchecked")
final class ORMSupport
  {
    private static Logger logger = LoggerFactory.getLogger(ORMSupport.class);
    static final int TRANS_SAVE_OBJECT = 0;
    static final int TRANS_DELETE_OBJECT = 3;
    static final int MAX_JOINS = 20;

    static Result queryObject(Database db, Object object, boolean idColumnsOnly, String externalClauses, Object[] parameters) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        if (object == null)
          throw new DatabaseException("object is null");

        Class cs = null;
        
        if (object instanceof Class)
          {
            cs = (Class)object;
            object = null;
          }
        else cs = object.getClass();
        
        logger.debug("Querying object: class {}", cs.getName());
        logger.debug("externalClauses = {}", externalClauses);
        logger.debug("parameters[] = {}", StringUtils.toString(parameters));
        logger.debug("IdColumnsOnly = {}", idColumnsOnly);

        StringBuilder sqlStatement = new StringBuilder(),
                     externalClausesStrBuf = null;
        List values = new ArrayList();
        
        if (externalClauses == null || !externalClauses.startsWith("select"))
          {
            StringBuilder fromStrBuf = new StringBuilder(), whereStrBuf = new StringBuilder();

            if (externalClauses != null)
              externalClausesStrBuf = new StringBuilder(externalClauses);
            
            processClasses(db, cs, object, true, idColumnsOnly, false, false, false, new QueryObjectHandler(db, fromStrBuf, whereStrBuf, externalClausesStrBuf, values));
              
            sqlStatement.append("select ").append("*");

            if (externalClauses == null || !externalClauses.startsWith("from"))
              {
                sqlStatement.append(" from ").append(fromStrBuf);

                if (whereStrBuf.length() > 0)
                  {
                    if (externalClauses == null || !externalClauses.startsWith("where"))
                      sqlStatement.append(" where ").append(whereStrBuf);
                  }
                else if (idColumnsOnly)
                  throw new DatabaseException("useIdColumnsOnly is defined, but there are no Id field values available");
              }
          }

        if (externalClauses != null)
          {
            sqlStatement.append(" ").append(externalClausesStrBuf);

            if (parameters != null)
              values.addAll(Arrays.asList(parameters));
          }

        if (values.size() > 0)
          return db.parameterizedQuery(sqlStatement.toString(), values.toArray()).setClass(cs);
        else
          return db.executeQuery(sqlStatement.toString()).setClass(cs);
      }

    static class QueryObjectHandler implements ClassHandler
      {
        Database db;
        String columnsStrBuf = "*";
        StringBuilder fromStrBuf, whereStrBuf, externalClausesStrBuf;
        String identifierQuoteString;
        Set<String> selectSet = new HashSet<String>(), whereSet = new HashSet<String>();
        Map<String, Table> lastTable = new HashMap<String, Table>();
        List values;
        
        public QueryObjectHandler(Database db, StringBuilder fromStrBuf, StringBuilder whereStrBuf, StringBuilder externalClausesStrBuf, List values) throws DatabaseException
          {
            this.db = db;
            this.fromStrBuf = fromStrBuf;
            this.whereStrBuf = whereStrBuf;
            this.externalClausesStrBuf = externalClausesStrBuf;
            this.values = values;
            
            this.identifierQuoteString = db.getMetaData().getIdentifierQuoteString();
          }
        
        public void processClass(Class objectClass, Object object, MetaData.Table table, int numberTables, int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException
          {
            fromStrBuf.append(fromStrBuf.length() > 0 ? ", " : "").append(table.getAbsoluteTableName(true));

            if (numberTables > 1)
              fromStrBuf.append(" t").append(tableNumber);

            if (externalClausesStrBuf != null)
              processExternalClauses(db, externalClausesStrBuf, table, objectClass, identifierQuoteString);

            processWhere(valuesMap, table, numberTables, tableNumber);
            processJoin(objectClass, table, numberTables, tableNumber);
          }
        
        void processWhere(Map valuesMap, MetaData.Table table, int numberTables, int tableNumber)
          {
            for (Iterator it = valuesMap.entrySet().iterator(); it.hasNext(); )
              {
                Map.Entry entry = (Map.Entry)it.next();
                MetaData.Table.Column column = (MetaData.Table.Column)entry.getKey();
                MetaData.Table.Key key = (MetaData.Table.Key)table.getImportedKeys().get(column.getColumnName());

                if (!whereSet.contains(column.getColumnName()) && column.isSearchable() && (key == null || !whereSet.contains(key.getForeignColumnName())))
                  {
                    whereSet.add(column.getColumnName());

                    if (whereStrBuf.length() > 0)
                      whereStrBuf.append(" and ");

                    if (numberTables > 1)
                      whereStrBuf.append("t").append(tableNumber).append(".");

                    Object obj = entry.getValue();

                    if (obj instanceof NullValue)
                      whereStrBuf.append(identifierQuoteString).append(column.getColumnName()).append(identifierQuoteString).append(" is null");
                    else
                      {
                        values.add(obj);

                        whereStrBuf.append(identifierQuoteString).append(column.getColumnName()).append(identifierQuoteString).append(hasWildCards(obj.toString()) ? " like ?" : " = ?");
                      }
                  }
              }
          }
        
        void processJoin(Class cs, MetaData.Table table, int numberTables, int tableNumber) throws DatabaseException
          {
            if (numberTables > 1)
              {
                MetaData.Table last = (MetaData.Table)lastTable.get("table");

                if (last != null)
                  {
                    Set keys = getMatchingImportedExportedKeys(table.getImportedKeys(), last.getExportedKeys());

                    if (keys.isEmpty())
                      throw new DatabaseException("The inheritance relation represented by " 
                                                  + cs.getName() 
                                                  + " does not have primary/foreign key relationships defined for the underlying"
                                                  + " tables (must have a FOREIGN KEY ... REFERENCES ... clause in table creation)");

                    MetaData.Table.Key key;
                    
                    for (Iterator it2 = keys.iterator(); it2.hasNext();)
                      {
                        key = (MetaData.Table.Key)it2.next();

                        whereStrBuf.append(whereStrBuf.length() > 0 ? " and t" : "t").append(tableNumber-1).append(".").append(identifierQuoteString).append(key.getForeignColumnName()).append(identifierQuoteString).append(" = t").append(tableNumber).append(".").append(identifierQuoteString).append(key.getLocalColumnName()).append(identifierQuoteString);
                      } 
                  }

                lastTable.put("table", table);
              }
          }
      }
    
    static Object loadObject(Result result, Object object) throws DatabaseException, SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException
      {
        Class objectClass = object.getClass();

        logger.debug("Loading object of class {}", objectClass.getName());
        
        if (object == null)
          throw new DatabaseException("data object is null");

        PersistenceManager.remove(object);
        
        LoadClassHandler classHandler = new LoadClassHandler(result);
        
        processClasses(result.getDatabase(), objectClass, object, false, false, false, false, true, classHandler);

        if (classHandler.canPersist)
          PersistenceManager.get(object).isPersistent = true;

        Boolean ignoreAssociations = result.getIgnoreAssociations() != null 
                                   ? result.getIgnoreAssociations()
                                   : result.getDatabase().getIgnoreAssociations() != null 
                                   ? result.getDatabase().getIgnoreAssociations()
                                   : result.getDatabase().getPersistentClassManager().get(objectClass).ignoreAssociations;

        if (!ignoreAssociations)
          loadAssociations(result.getDatabase(), object, result.getDatabase().getPersistentClassManager().get(objectClass).lazyLoading);

        return object;
      }
    
    static class LoadClassHandler implements ClassHandler
      {
        Result result;
        MetaData metaData;
        boolean hasTableInformation, canPersist;
        
        LoadClassHandler(Result result) throws DatabaseException
          {
            this.result = result;

            metaData = result.getDatabase().getMetaData();
          }
      
        public void processClass(final Class objectClass, final Object object, final MetaData.Table table, final int numberTables, final int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
          {
            ObjectFiller.fillObject(new GetHandler() 
              {
                public Object get(String key, Class objectType) throws ItemNotFoundException
                  {
                    boolean columnFound = false;
                    Object obj = null;
                    String name = null;

                    if ((name = result.getDatabase().getPersistentClassManager().getColumnMapping(objectClass, key)) == null)
                      name = StringUtils.camelCaseToLowerCaseUnderline(key);
                    else name = name.toLowerCase();

                    name = metaData.stripColumnName(name);

                    Integer columnNumber = result.getColumnNumber(name);

                    if (columnNumber == null)
                      {
                        name = metaData.stripColumnName(key).toLowerCase();
                        columnNumber = result.getColumnNumber(name);
                      }
                    
                    if (columnNumber != null)
                      try
                        {
                          obj = result.getColumnValue(objectType, columnNumber);
                          columnFound = true;
                        }
                      catch (Exception e) {} // don't care

                    if (!columnFound)
                      throw(new ItemNotFoundException());
                    
                    return obj;
                  }
              }, object, numberTables > 1 ? objectClass : null, true, false, null, true);

            if (table != null)
              {
                loadKeys(table, objectClass, object, tableNumber);
                canPersist = true;
              }
          }
        
        void loadKeys(MetaData.Table table, Class objectClass, Object object, int tableNumber) throws DatabaseException
          {
            Set ids = new HashSet();
            
            ids.addAll(table.getPrimaryKeys().keySet());
            ids.addAll(table.getImportedKeys().keySet());

            String columnName = null;
            Object value = null;
            
            for (Iterator it = ids.iterator(); it.hasNext();)
              {
                columnName = (String)it.next();

                Integer columnNumber = result.getColumnNumber(columnName.toLowerCase());

                if (columnNumber != null)
                  try
                    {
                      value = result.getColumnValue(columnNumber);

                      PersistenceManager.get(object).keyValues.put(columnName, value);
                    }
                  catch (Exception e2) {} // don't care
              }
          }
      }

    public static void loadAssociations(Database db, Object object, boolean lazyLoad) throws DatabaseException, SQLException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Class objectClass = object.getClass();
        Class parameterType = null;

        for (Method method : objectClass.getMethods())
          {
            if (method.getName().startsWith("set") && method.getParameterTypes().length > 0)
              {
                parameterType = method.getParameterTypes()[0];

                if (Collection.class.isAssignableFrom(parameterType))
                  loadCollection(db, objectClass, object, method, parameterType, lazyLoad, false);
                else if (parameterType.isArray())
                  loadArray(db, objectClass, object, method, parameterType);
                else if (!parameterType.getName().startsWith("java") && !parameterType.isPrimitive())
                  loadInstance(db, objectClass, object, method, parameterType);
              }
          }
      }

    static Collection loadCollection(Database db, Class objectClass, Object object, Method method, Class parameterType, boolean lazyLoad, boolean returnOnly) throws SQLException, DatabaseException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Type genericParameterType = method.getGenericParameterTypes()[0];

        if (genericParameterType instanceof ParameterizedType)
          {
            Class associationClass = (Class)((ParameterizedType)genericParameterType).getActualTypeArguments()[0];

            if (!associationClass.getName().startsWith("java") && !associationClass.isPrimitive())
              {
                String tableName = getTableNameFromClass(associationClass);

                if (db.getMetaData().getTable(db, tableName, associationClass) != null)
                  {
                    Object associationObject = null;
                    Collection collection = null;

                    if (parameterType.equals(List.class) || parameterType.equals(Collection.class) || parameterType.equals(ArrayList.class))
                      collection = lazyLoad ? new EjpList(db.getDatabaseManager(), objectClass, object, method, parameterType) : new ArrayList();
                    else if (parameterType.equals(Queue.class))
                      collection = lazyLoad ? new EjpQueue(db.getDatabaseManager(), objectClass, object, method, parameterType) : new LinkedList();
                    else if (parameterType.equals(Set.class) || parameterType.equals(HashSet.class))
                      collection = lazyLoad ? new EjpSet(db.getDatabaseManager(), objectClass, object, method, parameterType) : new HashSet();
                    else if (parameterType.equals(SortedSet.class) || parameterType.equals(TreeSet.class))
                      collection = lazyLoad ? new EjpSortedSet(db.getDatabaseManager(), objectClass, object, method, parameterType) : new TreeSet();
                    else 
                      {
                        lazyLoad = false;
                        collection = (Collection)parameterType.newInstance();
                      }

                    if (lazyLoad || copyAssociationIds(db, associationObject = associationClass.newInstance(), object, false))
                      {
                        Result result = null;
                        
                        if (!lazyLoad)
                          result = db.queryObject(associationObject);

                        try
                          {
                            if (collection != null)
                              {
                                if (! lazyLoad)
                                  {
                                    while (result.hasNext())
                                      collection.add(result.next());
                                  }
                                
                                if (!returnOnly && (lazyLoad || collection.size() > 0))
                                  method.invoke(object, new Object[] { collection });
                              }
                            
                            return collection;
                          }
                        finally
                          {
                            if (!lazyLoad)
                              result.close();
                          }
                      }
                    else
                      logger.debug("Warning: No value returned for association {} copying value(s) to {}", associationClass, objectClass);
                  }
                else
                  logger.debug("Warning: Could not locate table for {}" + associationClass);
              }
          }
        else
          logger.debug("Warning: {}: Collection will be ignored because it is not defined with a parameterized type", method.getName());
        
        return null;
      }

    static void loadArray(Database db, Class objectClass, Object object, Method method, Class parameterType) throws SQLException, DatabaseException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Class associationClass = parameterType.getComponentType();

        if (!associationClass.getName().startsWith("java") && !associationClass.isPrimitive())
          {
            String tableName = getTableNameFromClass(associationClass);

            if (db.getMetaData().getTable(db, tableName, associationClass) != null)
              {
                Object associationObject = associationClass.newInstance();

                if (copyAssociationIds(db, associationObject, object, false))
                  {
                    Result result = db.queryObject(associationObject);

                    try
                      {
                        List tmp = new ArrayList();

                        while (result.hasNext())
                          tmp.add(result.next());

                        if (tmp.size() > 0)
                          method.invoke(object, new Object[] { tmp.toArray((Object[])Array.newInstance(parameterType,tmp.size())) });
                      }
                    finally
                      {
                        result.close();
                      }
                  }
                else
                  logger.debug("Warning: No value returned for association {} copying value(s) to {}", associationClass, objectClass);
              }
            else
              logger.debug("Warning: Could not locate table for {}", associationClass);
          }
      }

    static void loadInstance(Database db, Class objectClass, Object object, Method method, Class parameterType) throws SQLException, DatabaseException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        String tableName = getTableNameFromClass(parameterType);
        Table associationTable = db.getMetaData().getTable(db, tableName, parameterType);

        if (associationTable != null)
          {
            Object associationObject = parameterType.newInstance();
            boolean importedAssociationFound = false;

            if (associationTable.getExportedKeys() != null)
              {
                String objectTableName = getTableNameFromClass(objectClass);
                Table objectTable = db.getMetaData().getTable(db, objectTableName, objectClass);

                for (Table.Key key : objectTable.getImportedKeys().values())
                  if (key.getForeignTableName().equalsIgnoreCase(associationTable.getTableName()))
                    importedAssociationFound = true;
              }

            if (copyAssociationIds(db, associationObject, object, importedAssociationFound))
              {
                Result result = db.queryObject(associationObject);

                try
                  {
                    Object obj = result.next();

                    if (obj != null)
                      method.invoke(object, new Object[] { obj });
                  }
                finally
                  {
                    result.close();
                  }
              }
            else
              logger.debug("Warning: No value returned for association {} copying value(s) to {}", parameterType, objectClass);
          }
        else
          logger.debug("Warning: Could not locate table for {}", parameterType);
      }

    static int objectTransaction(Database db, Object object, int transType, String externalClauses, Object[] parameters) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException
      {
        if (object == null)
          throw new DatabaseException("object is null");

        int returnValue = 0;
        boolean commit = false, rollback = false;

        if (db.getAutomaticTransactions() && db.getAutoCommit())
          {
            db.setAutoCommit(false);
            commit = true;
          }
        
        try
          {
            if (transType == TRANS_DELETE_OBJECT)
              returnValue = deleteObject(db, object, externalClauses, parameters);
            else
              returnValue = saveObject(db, transType, object, externalClauses, parameters);
          }
        catch (Exception e)
          {
            rollback = true;
            
            if (db.getAutomaticTransactions() && commit)
              db.rollback();
              
            throw new DatabaseException(e);
          }
        finally
          {
            if (db.getAutomaticTransactions() && commit)
              {
                if (!rollback)
                  db.commit();
                
                db.setAutoCommit(true);
              }

            Class objectClass = object.getClass();
            Boolean reloadAfterSave = db.getPersistentClassManager().get(objectClass).reloadAfterSave;

            if (!rollback && !db.isBatch() && transType != TRANS_DELETE_OBJECT && reloadAfterSave)
              {
                try
                  {
                    Result result = queryObject(db, object, true, null, (Object[])null);

                    try
                      {
                        if (result.hasNext())
                          {
                            Boolean ignoreAssociations = result.getIgnoreAssociations();
                            result.setIgnoreAssociations(true);
                            result.next(object);
                            result.setIgnoreAssociations(ignoreAssociations);
                          }
                      }
                    finally
                      {
                        result.close();
                      }
                  }
                catch (Exception e)
                  {
                    String message = "Could not reload object following save.  Can not provide persistence for this object."
                            + "\nYou can either setReloadAfterSave(false), add a primary key, or implement GeneratedKeys .";

                    if (e.getMessage() != null && e.getMessage().startsWith("useIdColumnsOnly")
                          && !db.getMetaData().supportsGeneratedKeys())
                      {
                        message += "\nYour database reports that it does not support retrieving auto-generated keys."
                                 + "\nTherefore, you can't make objects relying on auto-generated keys persistent following an insert."
                                 + "\nIn this case, only loaded objects can be persistent.";

                        throw new DatabaseException(message, e);
                      }
                    
                    logger.warn(message, e);
                  }
              }
          }
        
        return returnValue;
      }

    static int saveObject(Database db, int transType, Object object, String externalClauses, Object[] parameters) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
      {
        int returnValue = 0;
        Class objectClass = object.getClass();
        boolean externalWhere = (externalClauses != null && externalClauses.toLowerCase().startsWith("where")),
                globalUpdate = objectClass.isAnnotationPresent(GlobalUpdate.class);

        returnValue += saveAssociations(db, object, transType, true);

        if (PersistenceManager.isPersistent(object) || globalUpdate || externalWhere)
          returnValue += updateObject(db, object, externalClauses, parameters);
        else 
          returnValue += insertObject(db, object);

        returnValue += saveAssociations(db, object, transType, false);
        
        return returnValue;
      }

    public static int saveAssociations(Database db, Object object, int transType, boolean imports) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
      {
        int returnValue = 0;
        Class objectClass = object.getClass(), returnType = null;

        for (Method method : objectClass.getMethods())
          {
            if (method.getName().startsWith("get"))
              {
                returnType = method.getReturnType();

                if (Collection.class.isAssignableFrom(returnType))
                  returnValue = saveCollection(db, objectClass, object, method, returnType, transType, imports);
                else if (returnType.isArray())
                  returnValue = saveArray(db, objectClass, object, method, returnType, transType, imports);
                else if (!returnType.getName().startsWith("java") && !returnType.isPrimitive())
                  returnValue = saveInstance(db, objectClass, object, method, returnType, transType, imports);
              }
          }

        return returnValue;
      }

    static int saveCollection(Database db, Class objectClass, Object object, Method method, Class returnType, int transType, boolean imports) throws SQLException, DatabaseException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, InstantiationException
      {
        int returnValue = 0;

        if (!imports)
          {
            Type genericParameterType = method.getGenericReturnType();

            if (genericParameterType instanceof ParameterizedType)
              {
                Class associationClass = (Class)((ParameterizedType)genericParameterType).getActualTypeArguments()[0];

                if (!associationClass.getName().startsWith("java") && !associationClass.isPrimitive())
                  {
                    String associationTableName = getTableNameFromClass(associationClass);
                    Table associationTable = db.getMetaData().getTable(db, associationTableName, associationClass);

                    if (associationTable != null)
                      {
                        Object returnObject = method.invoke(object, new Object[] {});

                        if (returnObject != null)
                          {
                            if (!(returnObject instanceof EjpLoadable) || ((EjpLoadable)returnObject).isLoaded())
                              {
                                Iterator it = ((Collection)returnObject).iterator();
                                Object associationObject = null;

                                while (it.hasNext() && (associationObject = it.next()) != null)
                                  {
                                    if (copyAssociationIds(db, associationObject, object, false))
                                      returnValue += saveObject(db, transType, associationObject, null, null);
                                    else
                                      logger.debug("Warning: No value returned for association {} copying value(s) to {}", associationClass, objectClass);
                                  }
                              }
                          }
                        else
                          logger.debug("Warning: no value returned from {}", method);
                      }
                    else
                      logger.debug("Warning: Could not locate table for {}", associationClass);
                  }
              }
            else
              logger.debug("Warning: {} Collection will be ignored because it is not defined with a parameterized type", method.getName());
          }

        return returnValue;
      }

    static int saveArray(Database db, Class objectClass, Object object, Method method, Class returnType, int transType, boolean imports) throws SQLException, DatabaseException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, InstantiationException
      {
        int returnValue = 0;

        if (!imports)
          {
            Class associationClass = returnType.getComponentType();

            if (!associationClass.getName().startsWith("java") && !associationClass.isPrimitive())
              {
                String associationTableName = getTableNameFromClass(associationClass);
                Table associationTable = db.getMetaData().getTable(db, associationTableName, associationClass);

                if (associationTable != null)
                  {
                    Object returnObject = method.invoke(object, new Object[] {});

                    if (returnObject != null)
                      {
                        Object associationObjectArray[] = (Object[])returnObject;

                        for (int i2 = 0; i2 < associationObjectArray.length; i2++)
                          {
                            if (copyAssociationIds(db, associationObjectArray[i2], object, false))
                              returnValue += saveObject(db, transType, associationObjectArray[i2], null, null);
                            else
                              logger.debug("Warning: No value returned for association {} copying value(s) to {}", associationClass, objectClass);
                          }
                      }
                    else
                      logger.debug("Warning: no value returned from {}", method);
                  }
                else
                  logger.debug("Warning: Could not locate table for {}", associationClass);
              }
          }

        return returnValue;
      }

    static int saveInstance(Database db, Class objectClass, Object object, Method method, Class returnType, int transType, boolean imports) throws SQLException, DatabaseException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, InstantiationException
      {
        int returnValue = 0;
        String associationTableName = getTableNameFromClass(returnType);
        Table associationTable = db.getMetaData().getTable(db, associationTableName, returnType);

        if (associationTable != null)
          {
            boolean importedAssociationFound = false;

            if (associationTable.getExportedKeys() != null)
              {
                String objectTableName = getTableNameFromClass(objectClass);
                Table objectTable = db.getMetaData().getTable(db, objectTableName, objectClass);

                for (Table.Key key : objectTable.getImportedKeys().values())
                  if (key.getForeignTableName().equalsIgnoreCase(associationTable.getTableName()))
                    importedAssociationFound = true;
              }

            if (imports == importedAssociationFound)
              {
                Object associationObject = method.invoke(object, new Object[] {});

                if (associationObject != null)
                  {
                    boolean associationCopied = true;

                    if (!imports)
                      associationCopied = copyAssociationIds(db, associationObject, object, imports);

                    if (associationCopied)
                      {
                        returnValue = saveObject(db, transType, associationObject, null, null);

                        if (imports)
                          copyAssociationIds(db, object, associationObject, !imports);
                      }
                    else
                      logger.debug("Warning: No value returned for association {} copying value(s) to {}", returnType, objectClass);
                  }
                else
                  logger.debug("Warning: no value returned from {}", method);
              }
          }
        else
          logger.debug("Warning: Could not locate table for {}", returnType);

        return returnValue;
      }

    static int insertObject(Database db, Object object) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException
      {
        Class objectClass = object.getClass();

        logger.debug("inserting object of class {}", objectClass.getName());
        
        if (object == null)
          throw new DatabaseException("object is null");

        List returnValues = new ArrayList();

        processClasses(db, objectClass, object, true, false, false, false, false, new InsertClassHandler(db, returnValues));

        if (!db.isBatch())
          PersistenceManager.get(object).isPersistent = true;
        
        int returnValue = 0;
        
        for (int i = 0; i < returnValues.size(); i++)
          returnValue += ((Integer)returnValues.get(i)).intValue();
        
        return returnValue;
      }

    static class InsertClassHandler implements ClassHandler
      {
        String identifierQuoteString;
        List returnValues;
        Database db;
        
        InsertClassHandler(Database db, List returnValues) throws DatabaseException
          {
            this.db = db;
            this.returnValues = returnValues;
            this.identifierQuoteString = db.getMetaData().getIdentifierQuoteString();
          }
      
        public void processClass(Class objectClass, Object object, MetaData.Table table, int numberTables, int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
          {
            StringBuffer sqlStatement = new StringBuffer("insert into " + table.getAbsoluteTableName(true) + " ("),
                         columnsStrBuf = new StringBuffer(),
                         valuesStrBuf = new StringBuffer();
            List columnValues = new ArrayList(), keysRequested = null, keysReturned = null;
            
            processColumns(table, valuesMap, sqlStatement, columnsStrBuf, valuesStrBuf, columnValues);
            
            String dbUrl = db.getMetaData().getDatabaseUrl().toLowerCase(),
                   possibleGeneratedKey = table.getGeneratedKey();
            
            if (possibleGeneratedKey != null)
              {
                keysRequested = new ArrayList();
                keysRequested.add(possibleGeneratedKey);

                if (dbUrl.startsWith("jdbc:oracle"))
                  keysReturned = new ArrayList(keysRequested);
                else
                  keysReturned = new ArrayList();
              }
            
            returnValues.add(new Integer(db.parameterizedUpdate(sqlStatement.toString(), keysReturned, columnValues.toArray())));
            
            if (!db.isBatch())
              {
                processGeneratedKeys(table, object, dbUrl, keysRequested, keysReturned);
                addKeysToKeySet(table, object);
              }
          }
        
        void processColumns(MetaData.Table table, Map valuesMap, StringBuffer sqlStatement, StringBuffer columnsStrBuf, StringBuffer valuesStrBuf, List columnValues) throws DatabaseException
          {
            int readOnly = 0;
            Map.Entry entry = null;
            MetaData.Table.Column column = null;
            
            for (Iterator it = valuesMap.entrySet().iterator(); it.hasNext();)
              {
                entry = (Map.Entry)it.next();
                column = (MetaData.Table.Column)entry.getKey();

                if (!column.isReadOnly())
                  {
                    Object obj = entry.getValue();

                    if (!(obj instanceof NullValue))
                      {
                        columnsStrBuf.append(columnsStrBuf.length() > 0 ? ", " : "").append(identifierQuoteString).append(column.getColumnName()).append(identifierQuoteString);
                        columnValues.add(obj);
                      }
                  }
                else readOnly++;
              }
        
            if (columnValues.isEmpty())
              {
                if (readOnly > 0)
                  throw new DatabaseException("Table " + table.getAbsoluteTableName(false) + " appears to be readonly, might need to log in to the database");
                else
                  throw new DatabaseException("There must be some number of column values to insert");
              }

            sqlStatement.append(columnsStrBuf).append(") values(");

            for (Iterator it = columnValues.iterator(); it.hasNext(); it.next())
              valuesStrBuf.append((valuesStrBuf.length() > 0 ? ", " : "")).append("?");

            sqlStatement.append(valuesStrBuf).append(")");
          }

        void processGeneratedKeys(MetaData.Table table, Object object, String dbUrl, List keysRequested, List keysReturned) throws DatabaseException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
          {
            boolean success = false;
            
            if (object instanceof GeneratedKeys)
              success = ((GeneratedKeys)object).setGeneratedKeys(db, keysRequested, keysReturned);
            
            if (!success)
              {
                if (dbUrl.startsWith("jdbc:oracle") && keysReturned != null && keysReturned.size() > 0)
                  {
                    if (keysRequested.size() != keysReturned.size())
                      throw new DatabaseException("Auto-generated keys returned (" + keysReturned.size() 
                                                  + ") do not match the number of primary keys requested " + keysRequested
                                                  + "; try implementing GeneratedKeys");

                    setAutoGeneratedKeys(db, object, table, keysRequested, keysReturned);
                  }
                else if (keysReturned != null && !keysReturned.isEmpty() && keysRequested != null && !keysRequested.isEmpty())
                  {
                    setAutoGeneratedKeys(db, object, table, keysRequested, keysReturned);
                  }
                else if (table.getGeneratedKey() != null)
                  {
                    boolean queryExecuted = false;
                    Result result = null;

                    if (dbUrl.startsWith("jdbc:mysql"))
                      {
                        result = db.executeQuery("select LAST_INSERT_ID() as id ");
                        queryExecuted = true;
                      }
                    else if (dbUrl.startsWith("jdbc:derby") || dbUrl.startsWith("jdbc:db2"))
                      {
                        result = db.executeQuery("select IDENTITY_VAL_LOCAL() as id from " + table.getAbsoluteTableName(true));
                        queryExecuted = true;
                      }
                    else if (dbUrl.startsWith("jdbc:hsqldb") || dbUrl.startsWith("jdbc:h2"))
                      {
                        result = db.executeQuery("select IDENTITY() as id from " + table.getAbsoluteTableName(true));
                        queryExecuted = true;
                      }
                    else if (dbUrl.startsWith("jdbc:postgre"))
                      {
                        result = db.executeQuery("select currval('" + table.getAbsoluteTableName(true) + '_' + table.getGeneratedKey() + "_seq" + "') as id");
                        queryExecuted = true;
                      }

                    if (queryExecuted && result.hasNext() && result.next() != null)
                      {
                        keysReturned = new ArrayList();
                        keysReturned.add(result.getColumnValue("id"));

                        setAutoGeneratedKeys(db, object, table, keysRequested, keysReturned);
                      }
                  }
              }
          }
        
        void addKeysToKeySet(MetaData.Table table, Object object) throws DatabaseException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
          {
            Set ids = new HashSet();
            
            ids.addAll(table.getPrimaryKeys().keySet());
            ids.addAll(table.getImportedKeys().keySet());

            String columnName = null;
            Method method = null;
            Object value = null;

            for (Iterator it = ids.iterator(); it.hasNext();)
              {
                columnName = (String)it.next();
                method = getMatchingMethod(db, columnName, object, true);

                if (method != null)
                  {
                    if ((value = method.invoke(object, (Object[])null)) != null)
                      PersistenceManager.get(object).keyValues.put(columnName, value);
                  }
              }
          }
      }
    
    static void setAutoGeneratedKeys(Database db, Object object, MetaData.Table table, List keysRequested, List keysReturned) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        String key = null;
        Object value = null;
        Method method = null;
        Object convertedObject = null;
        
        for (int h = 0; h < keysReturned.size(); h++)
          {
            key = (String)keysRequested.get(h);
            value = keysReturned.get(h);
            
            method = getMatchingMethod(db, key, object, false);

            if (method != null)
              {
                convertedObject = ObjectConverter.convertObject(method.getParameterTypes()[0], value);

                if (convertedObject != null)
                  method.invoke(object, new Object[] { convertedObject });
              }
            
            PersistenceManager.get(object).keyValues.put(key, value);
          }
      }

    static int updateObject(Database db, Object object, String externalClauses, Object[] parameters) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Class objectClass = object.getClass();

        if (logger.isDebugEnabled())
          {
            logger.debug("updating object of class {}", objectClass.getName());
            logger.debug("externalClauses = {}", externalClauses);
            logger.debug("parameters[] = {}", StringUtils.toString(parameters));
          }

        if (object == null)
          throw new DatabaseException("object is null");

        List returnValues = new ArrayList();
        Map updatedKeys = new HashMap();
        
        processClasses(db, objectClass, object, true, false, false, true, false, new UpdateClassHandler(db, updatedKeys, returnValues, externalClauses, parameters));

        PersistenceManager.get(object).keyValues.putAll(updatedKeys);
        PersistenceManager.get(object).isPersistent = true;
          
        int returnValue = 0;
        
        for (int i = 0; i < returnValues.size(); i++)
          returnValue += ((Integer)returnValues.get(i)).intValue();
        
        return returnValue;
      }

    static class UpdateClassHandler implements ClassHandler
      {
        Database db;
        String externalClauses, identifierQuoteString;
        Map updatedKeys;
        Object[] parameters;
        List returnValues;
        
        UpdateClassHandler(Database db, Map updatedKeys, List returnValues, String externalClauses, Object[] parameters) throws DatabaseException
          {
            this.db = db;
            this.externalClauses = externalClauses;
            this.parameters = parameters;
            this.updatedKeys = updatedKeys;
            this.returnValues = returnValues;
            
            this.identifierQuoteString = db.getMetaData().getIdentifierQuoteString();
          }
        
        public void processClass(Class objectClass, Object object, MetaData.Table table, int numberTables, int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException
          {
            StringBuilder sqlStatement = new StringBuilder();
            List columnValues = new ArrayList(), whereValues = new ArrayList();
            StringBuffer columnsStrBuf = new StringBuffer(), whereStrBuf = new StringBuffer();
            Map.Entry entry = null;
            MetaData.Table.Column column = null;
            String columnName = null;
            Object obj = null;

            for (Iterator it = valuesMap.entrySet().iterator(); it.hasNext(); )
              {
                entry = (Map.Entry)it.next();
                column = (MetaData.Table.Column)entry.getKey();
                columnName = column.getColumnName();
                obj = entry.getValue();

                if (!column.isReadOnly())
                  {
                    if (obj instanceof NullValue)
                      columnsStrBuf.append(columnsStrBuf.length() > 0 ? ", " : "").append(identifierQuoteString).append(columnName).append(identifierQuoteString).append(" = null");
                    else
                      {
                        columnsStrBuf.append(columnsStrBuf.length() > 0 ? ", " : "").append(identifierQuoteString).append(columnName).append(identifierQuoteString).append(" = ?");
                        columnValues.add(obj);
                      }
                  }

                if (PersistenceManager.get(object).keyValues.get(columnName) != null)
                  updatedKeys.put(columnName, obj);

                if ((obj = PersistenceManager.get(object).keyValues.get(columnName)) != null)
                  if (column.isSearchable())
                    {
                      whereStrBuf.append(whereStrBuf.length() > 0 ? " and " : "").append(identifierQuoteString).append(columnName).append(identifierQuoteString).append(hasWildCards(obj.toString()) ? " like ?" : " = ?");
                      whereValues.add(obj);
                    }
              }

            sqlStatement.append("update ").append(table.getAbsoluteTableName(true)).append(" set ").append(columnsStrBuf);

            if (whereStrBuf.length() > 0 && (externalClauses == null || !externalClauses.startsWith("where")))
              sqlStatement.append(" where ").append(whereStrBuf);

            StringBuilder externalClausesStrBuf = null;
            
            if (externalClauses != null)
              processExternalClauses(db, externalClausesStrBuf = new StringBuilder(externalClauses), table, objectClass, identifierQuoteString);
            
            if (externalClauses != null)
              {
                sqlStatement.append(" ").append(externalClausesStrBuf);

                if (parameters != null)
                  whereValues.addAll(Arrays.asList(parameters));
              }

            if (columnValues.size() > 0 && (whereValues.size() > 0 || objectClass.isAnnotationPresent(GlobalUpdate.class)))
              {
                columnValues.addAll(whereValues);

                returnValues.add(new Integer(db.parameterizedUpdate(sqlStatement.toString(), columnValues.toArray())));
              }
            else
              throw new DatabaseException("Object is not updatable because it is not persistent, it does not have a where clause, and it does not extend GlobalUpdate");
          }
      }
    
    static int deleteObject(Database db, Object object, String externalClauses, Object[] parameters) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        if (object == null)
          throw new DatabaseException("object is null");

        Class objectClass = object.getClass();

        if (logger.isDebugEnabled())
          {
            logger.debug("deleting object of class {}", objectClass.getName());
            logger.debug("externalClauses = {}", externalClauses);
            logger.debug("parameters[] = {}", StringUtils.toString(parameters));
          }

        List returnValues = new ArrayList();
        
        processClasses(db, objectClass, object, true, false, true, false, false, new DeleteClassHandler(db, returnValues, externalClauses, parameters));

        PersistenceManager.remove(object);
          
        int returnValue = 0;
        
        for (int i = 0; i < returnValues.size(); i++)
          returnValue += ((Integer)returnValues.get(i)).intValue();
        
        return returnValue;
      }

    static class DeleteClassHandler implements ClassHandler
      {
        Database db;
        String identifierQuoteString, externalClauses;
        Object[] parameters;
        List returnValues;
        
        DeleteClassHandler(Database db, List returnValues, String externalClauses, Object[] parameters) throws DatabaseException
          {
            this.db = db;
            this.externalClauses = externalClauses;
            this.parameters = parameters;
            this.returnValues = returnValues;
            
            this.identifierQuoteString = db.getMetaData().getIdentifierQuoteString();
          }
      
        public void processClass(Class objectClass, Object object, MetaData.Table table, int numberTables, int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
          {
            StringBuilder sqlStatement = new StringBuilder(), whereStrBuf = new StringBuilder();
            List values = new ArrayList();
            Map.Entry entry = null;
            MetaData.Table.Column column = null;
            String columnName = null;
            Object obj = null;

            for (Iterator it = valuesMap.entrySet().iterator(); it.hasNext(); )
              {
                entry = (Map.Entry)it.next();
                column = (MetaData.Table.Column)entry.getKey();

                if (column.isSearchable())
                  {
                    columnName = column.getColumnName();
                    obj = entry.getValue();

                    if (!(obj instanceof NullValue) && (!PersistenceManager.isPersistent(object)
                      || (PersistenceManager.isPersistent(object) && (obj = PersistenceManager.get(object).keyValues.get(columnName)) != null)))
                        {
                          whereStrBuf.append(whereStrBuf.length() > 0 ? " and " : "").append(identifierQuoteString).append(columnName).append(identifierQuoteString).append(hasWildCards(obj.toString()) ? " like ?" : " = ?");
                          values.add(obj);
                        }
                  }
              }

            sqlStatement.append("delete ");

            if (externalClauses == null || !externalClauses.startsWith("from"))
              {
                sqlStatement.append(" from ").append(table.getAbsoluteTableName(true));

                if (whereStrBuf.length() > 0 && (externalClauses == null || !externalClauses.startsWith("where")))
                  sqlStatement.append(" where ").append(whereStrBuf);
              }
            
            StringBuilder externalClausesStrBuf = null;
            
            if (externalClauses != null)
              processExternalClauses(db, externalClausesStrBuf = new StringBuilder(externalClauses), table, objectClass, identifierQuoteString);

            if (externalClauses != null)
              {
                sqlStatement.append(" ").append(externalClausesStrBuf);

                if (parameters != null)
                  values.addAll(Arrays.asList(parameters));
              }

            if (values.size() > 0)
              returnValues.add(new Integer(db.parameterizedUpdate(sqlStatement.toString(), values.toArray())));
            else if (!(objectClass.isAnnotationPresent(GlobalDelete.class)))
              throw new DatabaseException("Object does not have a where clause and does not extend GlobalDelete");
            else
              returnValues.add(new Integer(db.executeUpdate(sqlStatement.toString())));
          }
      }

    static boolean copyAssociationIds(Database db, Object copyToObject, Object copyFromObject, boolean imported) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Set keys = getMatchingImportedExportedKeys(getImportedExportedKeys(db, copyToObject, imported),
                                                   getImportedExportedKeys(db, copyFromObject, !imported));
        boolean valueCopied = false;
        MetaData.Table.Key key = null;
        Method getMethod = null, setMethod = null;
        Object obj = null, convertedObject = null;

        if (!keys.isEmpty())
          {
            for (Iterator it = keys.iterator(); it.hasNext();)
              {
                key = (MetaData.Table.Key)it.next();

                getMethod = getMatchingMethod(db, key.getForeignColumnName(), copyFromObject, true);
                setMethod = getMatchingMethod(db, key.getLocalColumnName(), copyToObject, false);

                if (setMethod == null && getMethod == null)
                  {
                    PersistenceManager.get(copyToObject).keyValues.put(key.getLocalColumnName(), PersistenceManager.get(copyFromObject).keyValues.get(key.getForeignColumnName()));
                  }
                else if(setMethod == null)
                  {
                    PersistenceManager.get(copyToObject).keyValues.put(key.getLocalColumnName(), getMethod.invoke(copyFromObject, (Object[])null));
                  }
                else
                  {
                    obj = getMethod == null ? PersistenceManager.get(copyFromObject).keyValues.get(key.getForeignColumnName())
                                                   : getMethod.invoke(copyFromObject, (Object[])null);
                    
                    convertedObject = ObjectConverter.convertObject(setMethod.getParameterTypes()[0], obj);

                    setMethod.invoke(copyToObject, new Object[] { convertedObject });
                  }

                valueCopied = true;
              }
          }
        else
          {
            throw new DatabaseException("The association relation represented by " 
                                        + copyFromObject.getClass().getName() + " <-- " + copyToObject.getClass().getName()
                                        + " does not have primary/foreign key relationships defined for the underlying"
                                        + " tables (must have a FOREIGN KEY ... REFERENCES ... clause in table creation, see alter table)");
          }

        return valueCopied;
      }

    static Method getMatchingMethod(Database db, String columnName, Object object, boolean getMethod)
      {
        Class objectClass = object.getClass();
        Method methods[] = objectClass.getMethods();
        String matchName = MetaData.normalizeName(columnName);

        for (int i = 0; i < methods.length; i++)
          if (((getMethod && methods[i].getName().startsWith("get")) || (!getMethod &&  methods[i].getName().startsWith("set")))
            && MetaData.normalizeName(methods[i].getName().substring(3)).indexOf(matchName) > -1)
              return methods[i];

        String name = null;
        
        if ((name = db.getPersistentClassManager().getColumnMapping(objectClass, columnName)) != null)
          {
            matchName = name;
          }
        else if ((name = db.getPersistentClassManager().getReverseColumnMapping(objectClass, columnName, true)) != null)
          {
            matchName = name;
          }
        
        for (int i = 0; i < methods.length; i++)
          if (((getMethod && methods[i].getName().startsWith("get")) || (!getMethod && methods[i].getName().startsWith("set")))
            && MetaData.normalizeName(methods[i].getName().substring(3)).indexOf(matchName) > -1)
              return methods[i];

        return null;
      }

    static Set getMatchingImportedExportedKeys(Map associationKeys, Map objectKeys)
      {
        Set keys = new HashSet();
        MetaData.Table.Key foreignKey = null, localKey = null;

        for (Iterator it = associationKeys.entrySet().iterator(); it.hasNext();)
          {
            foreignKey = (MetaData.Table.Key)((Map.Entry)it.next()).getValue();

            if (foreignKey != null)
              {
                localKey = (MetaData.Table.Key)objectKeys.get(foreignKey.getForeignColumnName());

                if (localKey != null)
                  keys.add(foreignKey);
              }
          }

        return keys;
      }

    static Map getImportedExportedKeys(Database db, Object object, boolean exportedKeys) throws DatabaseException, SQLException
      {
        Map keys = new HashMap();
        Class objectClass = object.getClass();
        Package p = objectClass.getPackage();
        String tableName = null;
        MetaData.Table table = null;
        
        while (objectClass != null && p == null || (p != null && (!objectClass.getPackage().equals(ORMSupport.class.getPackage()) && !objectClass.getPackage().getName().equals("java.lang"))))
          {
            tableName = getTableNameFromClass(objectClass);
            table = db.getMetaData().getTable(db, tableName, objectClass);
            
            if (table == null)
              throw new DatabaseException("Table for " + objectClass + " is not locatable");
            
            if (exportedKeys)
              keys.putAll(table.getExportedKeys());
            else
              keys.putAll(table.getImportedKeys());
            
            objectClass = objectClass.getSuperclass();
            p = objectClass.getPackage();
          }
        
        return keys;
      }

    static interface ClassHandler
      {
        void processClass(Class objectClass, Object object, MetaData.Table table, int numberTables, int tableNumber, Map valuesMap) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException;
      }
    
    static void processClasses(Database db, Class objectClass, Object object, boolean tableRequired, boolean IdColumnsOnly, boolean baseTableOnly, boolean isUpdate, boolean loadOnly, ClassHandler ch) throws DatabaseException, SQLException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Class headClass = objectClass, baseClass = null;
        Package p = objectClass.getPackage();
        int tableNumber = 1;
        List classes = new ArrayList();
        boolean singleTable = objectClass.isAnnotationPresent(SingleTableInheritance.class),
                allFieldsSt = singleTable || objectClass.isAnnotationPresent(ConcreteTableInheritance.class);
        int numberOfTables = 0;
        
        while (p == null || (p != null && (!objectClass.getPackage().equals(ORMSupport.class.getPackage()) && !objectClass.getPackage().getName().equals("java.lang"))))
          {
            classes.add(objectClass);

            baseClass = objectClass;
            objectClass = objectClass.getSuperclass();
            
            p = objectClass.getPackage();
          }

        if (baseTableOnly || allFieldsSt)
          numberOfTables = 1;
        else
          numberOfTables = classes.size();
        
        for (int i = classes.size() - 1; i > -1; i--, tableNumber++)
          {
            String tableName = null;

            if (allFieldsSt)
              {
                objectClass = headClass;
                tableName = getTableNameFromClass(singleTable ? baseClass : headClass);
              }
            else
              {
                objectClass = (Class)classes.get(i);
                tableName = getTableNameFromClass(objectClass);
              }
            
            MetaData.Table table = db.getMetaData().getTable(db, tableName, objectClass);

            if (table == null && tableRequired)
              throw new DatabaseException("Table for " + objectClass + " is not locatable");

            Map valuesMap = null;

            if (!loadOnly)
              {
                valuesMap = new HashMap();

                if (table != null)
                  {
                    getSelectableKeyValues(db, valuesMap, table, object);

                    getValuesMap(db, valuesMap, table, objectClass, object, IdColumnsOnly, isUpdate, allFieldsSt);
                  }
              }
            
            ch.processClass(objectClass, object, table, numberOfTables, tableNumber, valuesMap);
            
            if (baseTableOnly || allFieldsSt)
              break;
          }
      }

    static void getSelectableKeyValues(Database db, Map valuesMap, MetaData.Table table, Object object) throws DatabaseException
      {
        MetaData.Table.Column column = null;
        Object obj = null;
        
        if (table.getPrimaryKeys() != null)
          for (String keyName : table.getPrimaryKeys().keySet())
            {
              column = table.getColumn(keyName);

              if (object != null)
                {
                  obj = PersistenceManager.get(object).keyValues.get(keyName);

                  if (obj != null)
                    valuesMap.put(column, obj);
                }
            }
        
        if (table.getImportedKeys() != null)
          for (String keyName : table.getImportedKeys().keySet())
            {
              column = table.getColumn(keyName);

              if (object != null)
                {
                  obj = PersistenceManager.get(object).keyValues.get(keyName);

                  if (obj != null)
                    valuesMap.put(column, obj);
                }
            }
      }

    static void getValuesMap(Database db, Map valuesMap, MetaData.Table table, Class objectClass, Object object, boolean IdColumnsOnly, boolean isUpdate, boolean allFieldsSti) throws IllegalAccessException, InvocationTargetException, DatabaseException
      {
        Method methods[] = objectClass.getMethods();
        ClassInformation ci = db.getPersistentClassManager().get(objectClass);
        MetaData.Table.Column column = null;
        String propertyName = null;
        Object value = null;

        for (int i = 0; i < methods.length; i++)
          {
            if (methods[i].getName().startsWith("get") && (allFieldsSti ||  methods[i].getDeclaringClass().equals(objectClass)) && methods[i].getParameterTypes().length == 0)
              {
                column = null;
                propertyName = methods[i].getName().substring(3);
                column = table.getColumn(db, propertyName, objectClass);

                if (column != null)
                  {
                    if (object != null)
                      {
                        value = methods[i].invoke(object, (Object[])null);

                        if (IdColumnsOnly == false || table.getPrimaryKeys().isEmpty() || column.isPrimaryKey())
                          {
                            if (value != null)
                              valuesMap.put(column, value);
                            else
                              {
                                if (isUpdate || ci.nullValuesToIncludeInQueries != null)
                                  if (isUpdate || ci.nullValuesToIncludeInQueries.contains(Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1)))
                                    valuesMap.put(column, new NullValue(column.getDataType()));
                              }
                          }
                      }
                  }
              }
          }
      }

    static void processExternalClauses(Database db, StringBuilder externalClausesStrBuf, MetaData.Table table, Class objectClass, String identifierQuoteString) throws DatabaseException
      {
        int pos = 0, pos2;
        String searchStr = null;
        MetaData.Table.Column column = null;

        while ((pos = externalClausesStrBuf.indexOf(":")) > -1)
          {
            pos2 = 0;

            for (pos2 = pos + 1; pos2 < externalClausesStrBuf.length() && "~`!@#$%^&*()-=+\\|]}[{'\";:/?.>,< ".indexOf(externalClausesStrBuf.charAt(pos2)) == -1; pos2++);

            searchStr = externalClausesStrBuf.substring(pos+1,pos2);
            column = table.getColumn(db, searchStr, objectClass);
            
            if (column != null)
              externalClausesStrBuf.replace(pos, pos2, identifierQuoteString + column.getColumnName() + identifierQuoteString);
          }
      }
        
    static boolean valueIsZeroOrFalse(Object object)
      {
        if (object instanceof Integer)
          return ((Integer)object).intValue() == 0;
        
        if (object instanceof Short)
          return ((Short)object).shortValue() == 0;
        
        if (object instanceof Long)
          return ((Long)object).longValue() == 0;
        
        if (object instanceof Float)
          return ((Float)object).floatValue() == 0.0;

        if (object instanceof Double)
          return ((Double)object).doubleValue() == 0.0;
        
        if (object instanceof Boolean)
          return ((Boolean)object).booleanValue() == false;
        
        return false;
      }
    
    static String getTableNameFromClass(Class objectClass)
      {
        String tableName = objectClass.getName();
        
        tableName = tableName.replace('$','.');
        
        if (tableName.lastIndexOf('.') != -1)
          tableName = tableName.substring(tableName.lastIndexOf('.') + 1);
        
        if (tableName.indexOf(';') != -1)
          tableName = tableName.substring(0,tableName.indexOf(';'));
        
        return tableName;
      }

    static boolean hasWildCards(String value)
      {
        if (value.indexOf('%') != -1 || value.indexOf('_') != -1)
          return  true;

        return false;
      }

    static class NullValue
      {
        int sqlType;

        NullValue(int sqlType) { this.sqlType = sqlType; }

        int getSqlType() { return sqlType; }
      }

    interface EjpLoadable extends Serializable
      {
        boolean isLoaded();
      }

    public static class EjpSortedSet implements SortedSet, EjpLoadable
      {
        transient DatabaseManager dbm;
        transient Class objectClass, parameterType;
        transient Method method;
        transient Object object;
        boolean loaded;
        SortedSet ejpSortedSet;

        public EjpSortedSet(DatabaseManager dbm, Class objectClass, Object object, Method method, Class parameterType)
          {
            this.dbm = dbm;
            this.objectClass = objectClass;
            this.object = object;
            this.method = method;
            this.parameterType = parameterType;
          }
        
        void init()
          {
            loaded = true;
            Database db = null;

            try
              {
                db = dbm.getDatabase();
                ejpSortedSet = (SortedSet)loadCollection(db, objectClass, object, method, parameterType, false, true);
              }
            catch (Exception e)
              {
                logger.error(e.toString(), e);
              }
            finally
              {
                db.close();
              }
          }

        public Comparator comparator() { if (!loaded) init(); return ejpSortedSet.comparator(); }
        public SortedSet subSet(Object fromElement, Object toElement) { if (!loaded) init(); return ejpSortedSet.subSet(fromElement, toElement); }
        public SortedSet headSet(Object toElement) { if (!loaded) init(); return ejpSortedSet.headSet(toElement); }
        public SortedSet tailSet(Object fromElement) { if (!loaded) init(); return ejpSortedSet.tailSet(fromElement); }
        public Object first() { if (!loaded) init(); return ejpSortedSet.first(); }
        public Object last() { if (!loaded) init(); return ejpSortedSet.last(); }
        public int size() { if (!loaded) init(); return ejpSortedSet.size(); }
        public boolean isEmpty() { if (!loaded) init(); return ejpSortedSet.isEmpty(); }
        public boolean contains(Object o) { if (!loaded) init(); return ejpSortedSet.contains(o); }
        public Iterator iterator() { if (!loaded) init(); return ejpSortedSet.iterator(); }
        public Object[] toArray() { if (!loaded) init(); return ejpSortedSet.toArray(); }
        public Object[] toArray(Object[] a) { if (!loaded) init(); return ejpSortedSet.toArray(a); }
        public boolean add(Object e) { if (!loaded) init(); return ejpSortedSet.add(e); }
        public boolean remove(Object o) { if (!loaded) init(); return ejpSortedSet.remove(o); }
        public boolean containsAll(Collection c) { if (!loaded) init(); return ejpSortedSet.containsAll(c); }
        public boolean addAll(Collection c) { if (!loaded) init(); return ejpSortedSet.addAll(c); }
        public boolean retainAll(Collection c) { if (!loaded) init(); return ejpSortedSet.retainAll(c); }
        public boolean removeAll(Collection c) { if (!loaded) init(); return ejpSortedSet.removeAll(c); }
        public void clear() { if (!loaded) init(); ejpSortedSet.clear(); }

        public boolean isLoaded() { return loaded; }
      }
    
    public static class EjpQueue implements Queue, EjpLoadable
      {
        transient DatabaseManager dbm;
        transient Class objectClass, parameterType;
        transient Method method;
        transient Object object;
        boolean loaded;
        Queue ejpQueue;

        public EjpQueue(DatabaseManager dbm, Class objectClass, Object object, Method method, Class parameterType)
          {
            this.dbm = dbm;
            this.objectClass = objectClass;
            this.object = object;
            this.method = method;
            this.parameterType = parameterType;
          }
        
        void init()
          {
            loaded = true;
            Database db = null;

            try
              {
                db = dbm.getDatabase();
                ejpQueue = (Queue)loadCollection(db, objectClass, object, method, parameterType, false, true);
              }
            catch (Exception e)
              {
                logger.error(e.toString(), e);
              }
            finally
              {
                db.close();
              }
          }

        public boolean add(Object e) { if (!loaded) init(); return ejpQueue.add(e); }
        public boolean offer(Object e) { if (!loaded) init(); return ejpQueue.offer(e); }
        public Object remove() { if (!loaded) init(); return ejpQueue.remove(); }
        public Object poll() { if (!loaded) init(); return ejpQueue.poll(); }
        public Object element() { if (!loaded) init(); return ejpQueue.element(); }
        public Object peek() { if (!loaded) init(); return ejpQueue.peek(); }
        public int size() { if (!loaded) init(); return ejpQueue.size(); }
        public boolean isEmpty() { if (!loaded) init(); return ejpQueue.isEmpty(); }
        public boolean contains(Object o) { if (!loaded) init(); return ejpQueue.contains(o); }
        public Iterator iterator() { if (!loaded) init(); return ejpQueue.iterator(); }
        public Object[] toArray() { if (!loaded) init(); return ejpQueue.toArray(); }
        public Object[] toArray(Object[] a) { if (!loaded) init(); return ejpQueue.toArray(a); }
        public boolean remove(Object o) { if (!loaded) init(); return ejpQueue.remove(o); }
        public boolean containsAll(Collection c) { if (!loaded) init(); return ejpQueue.containsAll(c); }
        public boolean addAll(Collection c) { if (!loaded) init(); return ejpQueue.addAll(c); }
        public boolean removeAll(Collection c) { if (!loaded) init(); return ejpQueue.removeAll(c); }
        public boolean retainAll(Collection c) { if (!loaded) init(); return ejpQueue.retainAll(c); }
        public void clear() { if (!loaded) init(); ejpQueue.clear(); }

        public boolean isLoaded() { return loaded; }
      }
    
    public static class EjpSet implements Set, EjpLoadable
      {
        transient DatabaseManager dbm;
        transient Class objectClass, parameterType;
        transient Method method;
        transient Object object;
        boolean loaded;
        Set ejpSet;

        public EjpSet(DatabaseManager dbm, Class objectClass, Object object, Method method, Class parameterType)
          {
            this.dbm = dbm;
            this.objectClass = objectClass;
            this.object = object;
            this.method = method;
            this.parameterType = parameterType;
          }
        
        void init()
          {
            loaded = true;
            Database db = null;

            try
              {
                db = dbm.getDatabase();
                ejpSet = (Set)loadCollection(db, objectClass, object, method, parameterType, false, true);
              }
            catch (Exception e)
              {
                logger.error(e.toString(), e);
              }
            finally
              {
                db.close();
              }
          }

        public int size() { if (!loaded) init(); return ejpSet.size(); }
        public boolean isEmpty() { if (!loaded) init(); return ejpSet.isEmpty(); }
        public boolean contains(Object o) { if (!loaded) init(); return ejpSet.contains(o); }
        public Iterator iterator() { if (!loaded) init(); return ejpSet.iterator(); }
        public Object[] toArray() { if (!loaded) init(); return ejpSet.toArray(); }
        public Object[] toArray(Object[] a) { if (!loaded) init(); return ejpSet.toArray(a); }
        public boolean add(Object e) { if (!loaded) init(); return ejpSet.add(e); }
        public boolean remove(Object o) { if (!loaded) init(); return ejpSet.remove(o); }
        public boolean containsAll(Collection c) { if (!loaded) init(); return ejpSet.containsAll(c); }
        public boolean addAll(Collection c) { if (!loaded) init(); return ejpSet.addAll(c); }
        public boolean retainAll(Collection c) { if (!loaded) init(); return ejpSet.retainAll(c); }
        public boolean removeAll(Collection c) { if (!loaded) init(); return ejpSet.removeAll(c); }
        public void clear() { if (!loaded) init(); ejpSet.clear(); }

        public boolean isLoaded() { return loaded; }
      }
    
    public static class EjpList implements List, EjpLoadable
      {
        transient DatabaseManager dbm;
        transient Class objectClass, parameterType;
        transient Method method;
        transient Object object;
        boolean loaded;
        List ejpList;

        public EjpList(DatabaseManager dbm, Class objectClass, Object object, Method method, Class parameterType)
          {
            this.dbm = dbm;
            this.objectClass = objectClass;
            this.object = object;
            this.method = method;
            this.parameterType = parameterType;
          }
        
        void init()
          {
            loaded = true;
            Database db = null;

            try
              {
                db = dbm.getDatabase();
                ejpList = (List)loadCollection(db, objectClass, object, method, parameterType, false, true);
              }
            catch (Exception e)
              {
                logger.error(e.toString(), e);
              }
            finally
              {
                db.close();
              }
          }

        public int size() { if (!loaded) init(); return ejpList.size(); }
        public boolean isEmpty() { if (!loaded) init(); return ejpList.isEmpty(); }
        public boolean contains(Object o) { if (!loaded) init(); return ejpList.contains(o); }
        public Iterator iterator() { if (!loaded) init(); return ejpList.iterator(); }
        public Object[] toArray() { if (!loaded) init(); return ejpList.toArray(); }
        public Object[] toArray(Object[] a) { if (!loaded) init(); return ejpList.toArray(a); }
        public boolean add(Object e) { if (!loaded) init(); return ejpList.add(e); }
        public boolean remove(Object o) { if (!loaded) init(); return ejpList.remove(o); }
        public boolean containsAll(Collection c) { if (!loaded) init(); return ejpList.containsAll(c); }
        public boolean addAll(Collection c) { if (!loaded) init(); return ejpList.addAll(c); }
        public boolean addAll(int index, Collection c) { if (!loaded) init(); return ejpList.addAll(index, c); }
        public boolean removeAll(Collection c) { if (!loaded) init(); return ejpList.removeAll(c); }
        public boolean retainAll(Collection c) { if (!loaded) init(); return ejpList.retainAll(c); }
        public void clear() { if (!loaded) init(); ejpList.clear(); }
        public Object get(int index) { if (!loaded) init(); return ejpList.get(index); }
        public Object set(int index, Object element) { if (!loaded) init(); return ejpList.set(index, element); }
        public void add(int index, Object element) { if (!loaded) init(); ejpList.add(index, element); }
        public Object remove(int index) { if (!loaded) init(); return ejpList.remove(index); }
        public int indexOf(Object o) { if (!loaded) init(); return ejpList.indexOf(o); }
        public int lastIndexOf(Object o) { if (!loaded) init(); return ejpList.lastIndexOf(o); }
        public ListIterator listIterator() { if (!loaded) init(); return ejpList.listIterator(); }
        public ListIterator listIterator(int index) { if (!loaded) init(); return ejpList.listIterator(index); }
        public List subList(int fromIndex, int toIndex) { if (!loaded) init(); return ejpList.subList(fromIndex, toIndex); }

        public boolean isLoaded() { return loaded; }
      }
  }
