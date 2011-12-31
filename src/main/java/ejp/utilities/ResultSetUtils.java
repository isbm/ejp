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

package ejp.utilities;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import ejp.*;

/**
 * This class provides several static utility methods for retrieving data from a result set.
 */

@SuppressWarnings("unchecked")
public class ResultSetUtils
  {
    /**
     * Returns an array of strings representing the column names in the result set.
     *
     * @param resultSet the result set
     *
     * @return an array of strings
     */
/*
    public static String[] getColumnNames(ResultSet resultSet) throws DatabaseException, SQLException
      {
        String names[] = null;
        
        if (resultSet != null)
          {
            names = new String[resultSet.getMetaData().getColumnCount()];
            
            for (int i = 0; i < names.length; i++)
              names[i] = resultSet.getMetaData().getColumnName(i+1);
          }
        
        return names;
      }
*/
    /**
     * Returns a map of column name and value pairs from the current row of the result set.
     * 
     * @param resultSet a valid result set
     * @param map the map to load with column name and value pairs
     *
     * @return the map that was passed in
     */
/*
    public static Map getRowDataMap(ResultSet resultSet, Map map) throws DatabaseException, SQLException
      {
        if (resultSet != null)
          for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++)
            map.put(resultSet.getMetaData().getColumnName(i+1), resultSet.getObject(i+1));
            
        return map;
      }
*/
    /**
     * Returns a collection of the current rows data.
     *
     * @param resultSet a valid result set
     * @param c the collection to load with the current rows data
     *
     * @return the collection that was passed in
     */
/*
    public static Collection getRowDataCollection(ResultSet resultSet, Collection c) throws DatabaseException, SQLException
      {
        if (resultSet != null)
          for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++)
            c.add(resultSet.getObject(i+1));
            
        return c;
      }
*/
    /**
     * Returns an array of objects obtained from the current rows data.
     *
     * @param resultSet a valid result set
     *
     * @return an array of objects
     */

    public static Object[] getRowDataObjects(ResultSet resultSet) throws DatabaseException, SQLException
      {
        Object objects[] = null;

        if (resultSet != null)
          {
            objects = new Object[resultSet.getMetaData().getColumnCount()];
            
            for (int i = 0; i < objects.length; i++)
              objects[i] = resultSet.getObject(i+1);
          }
        
        return objects;
      }

    /**
     * Loads a map with key and value pairs from the first and second column of each row in the result set.
     *
     * @param resultSet a valid result set
     * @param map the map to load with key and value pairs
     *
     * @return the map that was passed in
     */
    
    public static Map loadMap(ResultSet resultSet, Map map) throws DatabaseException, SQLException
      {
        return loadMap(resultSet, map, 1, 2);
      }
    
    /**
     * Loads a map with key and value pairs from keyColumn and valueColumn of each row in the result set.
     *
     * @param resultSet a valid result set
     * @param map the map to load with keyColumn and valueColumn
     * @param keyColumn the key column
     * @param valueColumn the value column
     *
     * @return the map that was passed in
     */
    
    public static Map loadMap(ResultSet resultSet, Map map, int keyColumn, int valueColumn) throws DatabaseException, SQLException
      {
        if (resultSet != null)
          while (resultSet.next())
            map.put(resultSet.getObject(keyColumn), resultSet.getObject(valueColumn));
      
        return map;
      }
    
    /**
     * Loads a collection with row data from the result set.  If the rows have more than one value, 
     * then a collection of collections (of the same type) is created, otherwise a collection of an 
     * object is created.
     *
     * @param resultSet a valid result set
     * @param c the collection to load with the result set data
     *
     * @return the collection that was passed in
     */
    
    public static Collection loadCollection(ResultSet resultSet, Collection c) throws DatabaseException, SQLException, InstantiationException, IllegalAccessException
      {
        return loadCollection(resultSet, c, true);
      }
    
    /**
     * Loads a collection with row data from the result set.  If the rows have more than one value, 
     * then a collection of collections (of the same type) is created, otherwise depending on the 
     * value of singleObject, a collection of a given object is created.
     *
     * @param resultSet a valid result set
     * @param c the collection to load with the result set data
     * @param singleObject if true and a row has a single column then an object will be added to the collection. 
     *                     If false then a collection of the row data is added to the collection passed in.
     *
     * @return the collection that was passed in
     */
    
    public static Collection loadCollection(ResultSet resultSet, Collection c, boolean singleObject) throws DatabaseException, SQLException, InstantiationException, IllegalAccessException
      {
        if (resultSet != null)
          {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (resultSet.next())
              {
                if (columnCount == 1 && singleObject)
                  c.add(resultSet.getObject(1));
                else
                  c.add(getRowDataObjects(resultSet));
              }
          }
        
        return c;
      }
  }
