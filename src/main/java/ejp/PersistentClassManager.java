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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to associate information regarding persistence preferences
 * for a Class whose objects will be involved with EJP persistence methods.  
 * This class has singleton properties that are global to a JVM.  If you have 
 * a need for a local impact you can override this with 
 * DatabaseManager.getPersistentClassManager() which is local to DatabaseManager.
 */

final public class PersistentClassManager
  {
    private static Logger logger = LoggerFactory.getLogger(PersistentClassManager.class);
    private static Map<Class, ClassInformation> classMap = Collections.synchronizedMap(new HashMap<Class, ClassInformation>());
    private static Boolean defaultReloadAfterSave = true, defaultIgnoreAssociations = false, defaultLazyLoading = true;

    /**
     * Define the table to use with this class. Overrides the default table search.
     *
     * @param cs the class of the persistent object to affect
     * @param tableMapping the table name as it exists in the database
     */

    public static void setTableMapping(Class cs, String tableMapping) 
      {
        logger.debug("tableMapping = {}", tableMapping);

        get(cs).tableMapping = tableMapping;
      }

    /**
     * Add column mappings from class property (method name without get/set) to table column name.  Overrides the default column search.
     *
     * @param cs the class of the persistent object to affect
     * @param columnMapping a map of class property to table column mappings
     */

    public static void setColumnMapping(Class cs, Map<String, String> columnMapping)
      {
        logger.debug("columnMapping = {}", columnMapping);

        get(cs).columnMapping = columnMapping;
      }

    /**
     * Define whether or not to include associations.
     *
     * @param ignoreAssociations ignore associations if true
     */

    public static void setIgnoreAssociations(Class cs, Boolean ignoreAssociations)
      {
        logger.debug("ignoreAssociations = {}", ignoreAssociations);

        get(cs).ignoreAssociations = ignoreAssociations;
      }

    /**
     * Define whether or not to include associations.
     *
     * @param ignoreAssociations ignore associations if true
     */

    public static void setDefaultIgnoreAssociations(Boolean defaultIgnoreAssociations)
      {
        logger.debug("ignoreAssociations = {}", defaultIgnoreAssociations);

        PersistentClassManager.defaultIgnoreAssociations = defaultIgnoreAssociations;
      }

    /**
     * Define whether or not to lazy load associations.
     *
     * @param cs
     * @param lazyLoading 
     */

    public static void setLazyLoading(Class cs, Boolean lazyLoading)
      {
        logger.debug("ignoreAssociations = {}", lazyLoading);

        get(cs).lazyLoading = lazyLoading;
      }

    /**
     * Define whether or not to lazy load associations.
     *
     * @param lazyLoading 
     */

    public static void setDefaultLazyLoading(Boolean defaultLazyLoading)
      {
        logger.debug("ignoreAssociations = {}", defaultLazyLoading);

        PersistentClassManager.defaultLazyLoading = defaultLazyLoading;
      }

    /**
     * Define a set of property names (method name without get/set) to include in generated SQL statements when null.
     *
     * @param nullValuesToIncludeInQueries a variable set of property names to include in generated SQL statements when null
     */
    public static void setNullValuesToIncludeInQueries(Class cs, String ... nullValuesToIncludeInQueries)
      {
        logger.debug("nullValuesToIncludeInQueries = {}", nullValuesToIncludeInQueries);

        Set<String> nullValues = new HashSet<String>();

        for (String value : nullValuesToIncludeInQueries)
          nullValues.add(Character.toLowerCase(value.charAt(0)) + value.substring(1));

        get(cs).nullValuesToIncludeInQueries = nullValues;
      }

    /**
     * Clear the set of previously defined property names (method name without get/set) to include in generated SQL statements when null.
     */
    public static void clearNullValuesToIncludeInQueries(Class cs) 
      {
        logger.debug("cs = {}", cs);

        get(cs).nullValuesToIncludeInQueries = null;
      }

    /**
     * Define a set of property names (method name without get/set) to include in generated SQL statements when null.
     *
     * @param nullValuesToIncludeInSaves a variable set of property names to include in generated SQL statements when null
     */
    @Deprecated
    public static void setNullValuesToIncludeInSaves(Class cs, String ... nullValuesToIncludeInSaves)
      {
        logger.debug("nullValuesToIncludeInSaves = {}", nullValuesToIncludeInSaves);

        //Set<String> nullValues = new HashSet<String>();

        //for (String value : nullValuesToIncludeInSaves)
        //  nullValues.add(Character.toLowerCase(value.charAt(0)) + value.substring(1));

        //get(cs).nullValuesToIncludeInSaves = nullValues;
      }

    /**
     * Clear the set of previously defined property names (method name without get/set) to include in generated SQL statements when null.
     */
    @Deprecated
    public static void clearNullValuesToIncludeInSaves(Class cs) 
      {
        logger.debug("cs = {}", cs);

        //get(cs).nullValuesToIncludeInSaves = null;
      }

    /**
     * If you don't have a lot of database triggers that can affect the value of the
     * saved object, and following a save the object will continue to match the value
     * in the database, you can set this value to false so that the object isn't
     * reloaded following saves.
     *
     * @param cs the class of the persistent object to affect
     * @param reloadAfterSave
     */

    public static void setReloadAfterSave(Class cs, Boolean reloadAfterSave) 
      {
        logger.debug("reloadAfterSave = {}", reloadAfterSave);

        get(cs).reloadAfterSave = reloadAfterSave;
      }

    /**
     * Set the default, for all objects, for reload after save.
     * 
     * @param reloadAfterSave
     */

    public static void setDefaultReloadAfterSave(Boolean reloadAfterSave) 
      {
        logger.debug("defaultReloadAfterSave = {}", reloadAfterSave);

        defaultReloadAfterSave = reloadAfterSave;
      }

    /**
     * Remove class from the persistent class manager (has the effect of resetting the class to new).
     *
     * @param cs the class of the persistent object to affect
     */
    public static void remove(Class cs) 
      {
        logger.debug("cs = {}", cs);

        classMap.remove(cs);
      }

    static String getColumnMapping(Class cs, String propertyName)
      {
        Map<String, String> columnMapping = get(cs).columnMapping;
        String columnName = columnMapping.get(propertyName);

        if (columnName == null)
          columnName = columnMapping.get(Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1));

        if (columnName == null)
          columnName = columnMapping.get(propertyName.toLowerCase());

        return columnName;
      }

    static String getReverseColumnMapping(Class cs, String propertyName, boolean buildMapping)
      {
        Map<String, String> reverseColumnMapping = get(cs).reverseColumnMapping;
        Map<String, String> columnMapping = get(cs).columnMapping;
        String columnName = reverseColumnMapping.get(propertyName);

        if (columnName == null)
          columnName = reverseColumnMapping.get(Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1));

        if (columnName == null)
          columnName = reverseColumnMapping.get(propertyName.toLowerCase());

        // Build first time through
        if (buildMapping && columnName == null)
          {
            reverseColumnMapping.clear();

            for (String name : columnMapping.keySet())
              reverseColumnMapping.put(columnMapping.get(name), name);

            columnName = getReverseColumnMapping(cs, propertyName, false);
          }

        return columnName;
      }

    static ClassInformation get(Class cs)
      {
        ClassInformation p = classMap.get(cs);

        if (p == null)
          classMap.put(cs, p = new ClassInformation(defaultReloadAfterSave, defaultIgnoreAssociations, defaultLazyLoading));

        return p;
      }

    static class ClassInformation
      {
        Boolean reloadAfterSave, ignoreAssociations, lazyLoading;
        String tableMapping;
        Set<String> nullValuesToIncludeInQueries;
        Map<String, String> columnMapping = new HashMap<String, String>(),
                            reverseColumnMapping = new HashMap<String, String>();
        
        ClassInformation(Boolean defaultReloadAfterSave, Boolean defaultIgnoreAssociations, Boolean defaultLazyLoading)
          {
            reloadAfterSave = defaultReloadAfterSave;
            ignoreAssociations = defaultIgnoreAssociations;
            lazyLoading = defaultLazyLoading;
          }
      }
  }
