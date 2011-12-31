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

import ejp.PersistentClassManager.ClassInformation;
import javax.naming.NamingException;
import ejp.utilities.XMLParser;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import ejp.utilities.StringUtils;
import ejp.utilities.XMLParserException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import ejp.utilities.ResultSetUtils;
import java.util.HashSet;
import java.util.Set;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The DatabaseManager is the entry point to EJP and provides ejp.Database 
 * instances, as well as, pooling management for database handlers and 
 * connection instances.  It also provides access to XML defined databases, 
 * logging, sql statements, and object-oriented database access.  An application 
 * can have any number of DatabaseManagers, but only one is typically needed.
 */

@SuppressWarnings("unchecked")
public final class DatabaseManager
  {
    private static final int CONNECTION_SOURCE_IS_UNDEFINED = 0;
    private static final int CONNECTION_SOURCE_IS_JNDI = 1;
    private static final int CONNECTION_SOURCE_IS_DATA_SOURCE = 2;
    private static final int CONNECTION_SOURCE_IS_DRIVER_MANAGER = 3;
    
    private static Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    private static Map dbDefinitionsMap = new HashMap();
    
    private String databaseName, databaseDriver, databaseUrl, databaseUsername, databasePassword, catalogPattern, schemaPattern;
    private int maxPoolSize, databasesAllocated, connectionSourceType = CONNECTION_SOURCE_IS_UNDEFINED;
    private Integer fetchSize, maxRows, resultSetType, resultSetConcurrency;
    private List connectionsList = Collections.synchronizedList(new ArrayList()),
                 databaseFreePool = Collections.synchronizedList(new ArrayList());
    private DataSource dataSource;
    private boolean isClosed, automaticTransactions = true;
    DatabaseManager.PersistentClassManager persistentClassManager = new PersistentClassManager();

    static
      {
        try
          {
            URL url = ClassLoader.getSystemResource("databases.xml");
            boolean validate = false;
            
            if (url == null)
              {
                url = ClassLoader.getSystemResource("databases_v.xml");
                validate = true;
              }
            
            if (url != null)
              new LoadXMLDefinition(url, validate);
          }
        catch (Exception e)
          {
            logger.error(e.toString(), e);
          }
      }

    static class LoadXMLDefinition extends XMLParser
      {
        LoadXMLDefinition(URL fileUrl, boolean validate) throws IOException, XMLParserException
          {
            super(fileUrl, validate);
          }
        
        public void processXML(Element e)
          {
            NodeList nl = e.getElementsByTagName("database");

            for (int i = 0; i < nl.getLength(); i++)
              {
                Element e2 = (Element)nl.item(i);

                String name = StringUtils.emptyToDefault(e2.getAttribute("name"),null),
                       useJndi = StringUtils.emptyToDefault(e2.getAttribute("useJndi"),null),
                       poolSize = StringUtils.emptyToDefault(e2.getAttribute("poolSize"),"10"),
                       driver = StringUtils.emptyToDefault(e2.getAttribute("driver"),null),
                       url = StringUtils.emptyToDefault(e2.getAttribute("url"),null),
                       catalogPattern = StringUtils.emptyToDefault(e2.getAttribute("catalogPattern"),null),
                       schemaPattern = StringUtils.emptyToDefault(e2.getAttribute("schemaPattern"),null),
                       user = StringUtils.emptyToDefault(e2.getAttribute("username"),null),
                       password = StringUtils.emptyToDefault(e2.getAttribute("password"),null);

                DefinedDatabase dd = new DefinedDatabase(name, driver, url, catalogPattern, schemaPattern, user, password, new Integer(poolSize).intValue(), useJndi == null ? false : new Boolean(useJndi).booleanValue());

                dbDefinitionsMap.put(name, dd);

                logger.debug(dd.toString());
              }
          }
      }

    static class DefinedDatabase
      {
        String name, driver, url, catalogPattern, schemaPattern, user, password;
        int poolSize;
        boolean useJndi;
        
        DefinedDatabase(String name, String driver, String url, String catalogPattern, String schemaPattern, String user, String password, int poolSize, boolean useJndi)
          {
            this.name = name;
            this.driver = driver;
            this.url = url;
            this.catalogPattern = catalogPattern;
            this.schemaPattern = schemaPattern;
            this.user = user;
            this.password = password;
            this.poolSize = poolSize;
            this.useJndi = useJndi;
          }
        
        String getName() { return name; }
        String getDriver() { return driver; }
        String getUrl() { return url; }
        String getCatalogPattern() { return catalogPattern; }
        String getSchemaPattern() { return schemaPattern; }
        String getUsername() { return user; }
        String getPassword() { return password; }
        int getPoolSize() { return poolSize; }
        boolean useJndi() { return useJndi; }
        
        public String toString()
          {
            if (useJndi)
              return "Name: " + name + ", poolsize = " + poolSize + ", useJndi = true, url = " + url + ", catalogPattern = " + catalogPattern + ", schemaPattern = " + schemaPattern + ", user = " + user + ", password = " + password;
            else
              return "Name: " + name + ", poolsize = " + poolSize + ", driver = " + driver + ", url = " + url + ", catalogPattern = " + catalogPattern + ", schemaPattern = " + schemaPattern + ", user = " + user + ", password = " + password;
          }
      }

    /**
     * Returns a DatabaseManager instance as defined in the databases.xml file.  The format
     * of the databases file, which is found via ClassLoader.getSystemResource(), is:
     * 
     * <pre>
     *     &lt;databases&gt;
     *         &lt;database name="" useJndi="" url="" [poolSize=""] [catalogPattern=""] [schemaPattern=""] [user=""] [password=""] /&gt;
     *         &lt;database name="" driver="" url="" [poolSize=""] [catalogPattern=""] [schemaPattern=""] [user=""] [password=""] /&gt;
     *     &lt;/databases&gt;
     * </pre>
     * 
     * 
     * @param dbName is the name defined in the database element
     * @return an instance os DatabaseManager, or null if not defined
     * @throws DatabaseException
     */
    
    public static DatabaseManager getDatabaseManager(String dbName)// throws DatabaseException
      {
        DefinedDatabase definedDatabase = (DefinedDatabase)dbDefinitionsMap.get(dbName);
        DatabaseManager databaseManager = null;

        if (logger.isDebugEnabled())
          if (definedDatabase != null)
            logger.debug("Defined database found: {}", definedDatabase);
          else
            logger.debug("Defined database was not found: name = {}", dbName);
        
        if (definedDatabase != null)
          {
            if (definedDatabase.useJndi())
              databaseManager = new DatabaseManager(dbName, definedDatabase.getPoolSize(), definedDatabase.getUrl());
            else
              {
                if (definedDatabase.getUsername() == null)
                  databaseManager = new DatabaseManager(dbName, definedDatabase.getPoolSize(), definedDatabase.getDriver(), definedDatabase.getUrl());
                else
                  databaseManager = new DatabaseManager(dbName, definedDatabase.getPoolSize(), definedDatabase.getDriver(), definedDatabase.getUrl(), definedDatabase.getUsername(), definedDatabase.getPassword());
              }

            if (definedDatabase.getCatalogPattern() != null)
              databaseManager.setCatalogPattern(definedDatabase.getCatalogPattern());

            if (definedDatabase.getSchemaPattern() != null)
              databaseManager.setSchemaPattern(definedDatabase.getSchemaPattern());
          }
        
        return databaseManager;
      }

    /**
     * Create a DatabaseManager instance using JNDI.
     * 
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param jndiUri the JNDI URI
     */
    
    public static DatabaseManager getDatabaseManager(String databaseName, int poolSize, String jndiUri)
      {
        return new DatabaseManager(databaseName, poolSize, jndiUri);
      }
    
    /**
     * Create a DatabaseManager instance using JNDI.
     * 
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param jndiUri the JNDI URI
     * @param username the username to use for signon
     * @param password the password to use for signon
     */
    
    public static DatabaseManager getDatabaseManager(String databaseName, int poolSize, String jndiUri, String username, String password)
      {
        return new DatabaseManager(databaseName, poolSize, jndiUri, username, password);
      }
    
    /**
     * Create a DatabaseManager instance using a supplied database driver.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param driver the database driver class name
     * @param url the driver oriented database url
     */
    
    public static DatabaseManager getDatabaseManager(String databaseName, int poolSize, String driver, String url)
      {
        return new DatabaseManager(databaseName, poolSize, driver, url);
      }
    
    /**
     * Create a DatabaseManager instance using a supplied database driver.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param driver the database driver class name
     * @param url the driver oriented database url
     * @param username the username to use for signon
     * @param password the password to use for signon
     */
    
    public static DatabaseManager getDatabaseManager(String databaseName, int poolSize, String driver, String url, String username, String password)
      {
        return new DatabaseManager(databaseName, poolSize, driver, url, username, password);
      }
    
    /**
     * Create a DatabaseManager instance using a supplied DataSource.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param dataSource the data source that supplies connections
     */
    
    public static DatabaseManager getDatabaseManager(String databaseName, int poolSize, DataSource dataSource)
      {
        return new DatabaseManager(databaseName, poolSize, dataSource);
      }
    
    /**
     * Create a DatabaseManager instance using a supplied DataSource.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param dataSource the data source that supplies connections
     */
    
    public DatabaseManager(String databaseName, int poolSize, DataSource dataSource)
      {
        logger.debug("Creating new DatabaseManager: name = {}, poolSize = {}", databaseName, poolSize);
      
        this.databaseName = databaseName;
        this.maxPoolSize = poolSize;
        this.dataSource = dataSource;
        this.connectionSourceType = CONNECTION_SOURCE_IS_DATA_SOURCE;
      }
    
    /**
     * Create a DatabaseManager instance using JNDI.
     * 
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param jndiUri the JNDI URI
     */
    
    public DatabaseManager(String databaseName, int poolSize, String jndiUri)
      {
        logger.debug("Creating new DatabaseManager: name = {}, poolSize = {}, jndiUrl = {}", new Object[] {databaseName, poolSize, jndiUri});
      
        this.databaseName = databaseName;
        this.maxPoolSize = poolSize;
        this.databaseUrl = jndiUri;
        this.connectionSourceType = CONNECTION_SOURCE_IS_JNDI;
      }
    
    /**
     * Create a DatabaseManager instance using JNDI.
     * 
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param jndiUri the JNDI URI
     * @param username the username to use for signon
     * @param password the password to use for signon
     */
    
    public DatabaseManager(String databaseName, int poolSize, String jndiUri, String username, String password)
      {
        logger.debug("Creating new DatabaseManager: name = {}, poolSize = {}, jndiUrl = {}, username = {}, password = {}",
                     new Object[] {databaseName, poolSize, jndiUri, username, password});
      
        this.databaseName = databaseName;
        this.maxPoolSize = poolSize;
        this.databaseUrl = jndiUri;
        this.databaseUsername = username;
        this.databasePassword = password;
        this.connectionSourceType = CONNECTION_SOURCE_IS_JNDI;
      }
    
    /**
     * Create a DatabaseManager instance using a supplied database driver.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param driver the database driver class name
     * @param url the driver oriented database url
     */
    
    public DatabaseManager(String databaseName, int poolSize, String driver, String url)
      {
        logger.debug("Creating new DatabaseManager: name = {}, poolSize = {}, driver = {}, url = {}",
                     new Object[] {databaseName, poolSize, driver, url});

        this.databaseName = databaseName;
        this.maxPoolSize = poolSize;
        this.databaseDriver = driver;
        this.databaseUrl = url;
        this.connectionSourceType = CONNECTION_SOURCE_IS_DRIVER_MANAGER;
      }
    
    /**
     * Create a DatabaseManager instance using a supplied database driver.
     *
     * @param databaseName the name to associate with the DatabaseManager instance
     * @param poolSize the number of instances to manage
     * @param driver the database driver class name
     * @param url the driver oriented database url
     * @param username the username to use for signon
     * @param password the password to use for signon
     */
    
    public DatabaseManager(String databaseName, int poolSize, String driver, String url, String username, String password)
      {
        logger.debug("Creating new DatabaseManager: name = {}, poolSize = {}, driver = {}, url = {}, username = {}, password = {}",
                     new Object[] {databaseName, poolSize, driver, url, username, password});

        this.databaseName = databaseName;
        this.maxPoolSize = poolSize;
        this.databaseDriver = driver;
        this.databaseUrl = url;
        this.databaseUsername = username;
        this.databasePassword = password;
        this.connectionSourceType = CONNECTION_SOURCE_IS_DRIVER_MANAGER;
      }

    /**
     * Returns the database name.
     * @return the database name
     */
    
    public String getDatabaseName() { return databaseName; }
    
    /**
     * Closes all resources associated with the DatabaseManager.  If the DatabaseManager 
     * is managing pooled connections via JNDI, then this method does nothing.  However, 
     * if the connections are allocated by the manager (non-JNDI), they will be closed.
     */
    public void close() throws DatabaseException
      {
        if (!isClosed)
          try
            {
              if (getDatabase().getConnection().getMetaData().getURL().indexOf("hsql") != -1)
                getDatabase().executeUpdate("shutdown;");

              if (connectionSourceType == CONNECTION_SOURCE_IS_DRIVER_MANAGER)
                synchronized(connectionsList)
                  {
                    for (Iterator it = connectionsList.iterator(); it.hasNext();)
                      ((Connection)it.next()).close();
                  }
              
              isClosed = true;
            }
          catch (Exception e)
            {
              throw new DatabaseException(e);
            }
      }

    /**
     * Returns true if the database manager is closed, false otherwise.
     *
     * @return true or false
     */
    
    public boolean isClosed() { return isClosed; }

    /**
     * Sets the logging level to the given level.
     *
     * @param level an instance of java.util.logging.Level
     * @deprecated No longer used see slf4j.org
     */
    @Deprecated
    public static void setLogLevel(java.util.logging.Level level) {}
    
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
            Database db = getDatabase();

            try
              {
                return db.getMetaData(); 
              }
            finally
              {
                db.close();
              }
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
    
    
    public void setFetchSize(int fetchSize)// throws DatabaseException
      {
        logger.debug("Setting fetch size to {}", fetchSize);
        
        this.fetchSize = fetchSize; 
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
     * See same in java.sql.Statement
     *
     * @see java.sql.Statement
     */
    
    public void setMaxRows(int maximumResultRows)// throws DatabaseException
      {
        logger.debug("Setting maximum result rows to {}", maximumResultRows);
        
        this.maxRows = maximumResultRows; 
      }
    
    /**
     * Defaults to ResultSet.TYPE_SCROLL_INSENSITIVE.
     * 
     * @see java.sql.Connection
     * @see java.sql.ResultSet
     *
     * @throws DatabaseException
     */

    public void setResultSetType(int resultSetType)// throws DatabaseException
      {
        logger.debug("Setting result set type to {}", resultSetType);
        
        this.resultSetType = resultSetType;
      }

    /**
     * Defaults to ResultSet.CONCUR_READ_ONLY.
     * 
     * @see java.sql.Connection
     * @see java.sql.ResultSet
     *
     * @throws DatabaseException
     */
    
    public void setResultSetConcurrency(int resultSetConcurrency)// throws DatabaseException
      {
        logger.debug("Setting result set concurrency to {}", resultSetConcurrency);
        
        this.resultSetConcurrency = resultSetConcurrency;
      }

    /**
     * Returns an instance of the defined database.
     *
     * @return an instance of Database
     * @throws DatabaseException
     */
    
    public synchronized Database getDatabase() throws DatabaseException
      {
        try
          {
            Database db = null;

            logger.debug("Retrieving database");
            
            if (databaseFreePool.size() > 0)
              {
                db = (Database)databaseFreePool.remove(0);

                logger.debug("Database allocated from free pool");
              }
            else if (databaseFreePool.size() < maxPoolSize)
              {
                db = new Database(this);
                db.setDatabaseName(databaseName);

                if (connectionSourceType == CONNECTION_SOURCE_IS_DRIVER_MANAGER)
                  {
                    Class.forName(databaseDriver);
                    
                    Connection connection = null;

                    if (databaseUsername != null)
                      connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);
                    else
                      connection = DriverManager.getConnection(databaseUrl);
                    
                    db.setConnection(connection);
                    connectionsList.add(connection);
                  }
              }
            else throw new DatabaseException("Database pool is empty");

            if (connectionSourceType != CONNECTION_SOURCE_IS_DRIVER_MANAGER)
              {
                try
                  {
                    if (connectionSourceType == CONNECTION_SOURCE_IS_JNDI)
                      {
                        if (databaseUsername != null)
                          db.setConnection(((DataSource)new InitialContext().lookup("java:comp/env/" + databaseUrl)).getConnection(databaseUsername,databasePassword));
                        else
                          db.setConnection(((DataSource)new InitialContext().lookup("java:comp/env/" + databaseUrl)).getConnection());
                      }
                    else if (connectionSourceType == CONNECTION_SOURCE_IS_DATA_SOURCE)
                      db.setConnection(dataSource.getConnection());
                    else throw new DatabaseException("connectionSourceType = " + connectionSourceType);
                  }
                catch (NamingException ex)
                  {
                    throw new DatabaseException("Error for database = " + databaseUrl, ex);
                  }
              }

            logger.debug("Databases allocated = {}", ++databasesAllocated);

            db.initDatabase();
            db.setCatalogPattern(catalogPattern);
            db.setSchemaPattern(schemaPattern);
            db.setFetchSize(fetchSize);
            db.setMaxRows(maxRows);
            db.setResultSetType(resultSetType);
            db.setResultSetConcurrency(resultSetConcurrency);
            db.setAutomaticTransactions(automaticTransactions);

            return db;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
      }

    void releaseDatabase(Database db)
      {
        if (connectionSourceType != CONNECTION_SOURCE_IS_DRIVER_MANAGER)
          try { db.getConnection().close(); } 
          catch (Exception e) 
            {
              logger.error(e.toString(), e);
            }

        databaseFreePool.add(db);

        logger.debug("Database added to free pool; size is now = {}, max size = {}", databaseFreePool.size(), maxPoolSize);
        logger.debug("Databases allocated = {}", --databasesAllocated);
      }
    
    /**
     * Builds a select query from an objects values, and then loads the
     * object with the result.
     * 
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     Result result = db.queryObject(object);
     * 
     *     if (result.hasNext())
     *       return result.next(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
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
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *
     * try
     *   {
     *     Result result = db.queryObject(MyClass.class);
     *
     *     if (result.hasNext())
     *       return result.next();
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
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
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     Result result = db.queryObject(object, externalClauses, externalClausesParameters);
     * 
     *     if (result.hasNext())
     *       return result.next(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
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
        Database db = getDatabase();
        
        try
          {
            Result<T> result = db.queryObject(object, externalClauses, externalClausesParameters);
            
            if (result.hasNext())
              return result.next(object);
          }
        finally
          {
            db.close();
          }
        
        return null;
      }

    /**
     * Builds a select query from a class, and then loads an instance of the
     * class with the result.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *
     * try
     *   {
     *     Result result = db.queryObject(MyClass.class);
     *
     *     if (result.hasNext())
     *       return result.next();
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     *
     * @param cs the class to base the load on
     *
     * @return returns the object passed in
     *
     * @throws DatabaseException
     */

    public <T> T loadObject(Class<T> cs, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Database db = getDatabase();

        try
          {
            Result<T> result = db.queryObject(cs, externalClauses, externalClausesParameters);

            if (result.hasNext())
              return result.next();
          }
        finally
          {
            db.close();
          }

        return null;
      }

    /**
     * Builds a select query from an objects values, and then loads a collection
     * with the result.
     * 
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *   
     * try
     *   {
     *     return db.queryObject(object).loadObjects(collection, object.getClass());
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     *   
     * return collection;
     * </pre>
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
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *
     * try
     *   {
     *     return db.queryObject(MyObj.class).loadObjects(collection, MyObj.class);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     *
     * return collection;
     * </pre>
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
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *   
     * try
     *   {
     *     return db.queryObject(object, externalClauses, externalClausesParameters).loadObjects(collection, object.getClass());
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     *   
     * return collection;
     * </pre>
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
        Database db = getDatabase();

        try
          {
            return db.queryObject(object, externalClauses, externalClausesParameters).loadObjects(collection, (Class<T>)object.getClass());
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Builds a select query from an objects class, and then loads a collection
     * with the result.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     *
     * try
     *   {
     *     return db.queryObject(MyObj.class, externalClauses, externalClausesParameters).loadObjects(collection, MyObj.class);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     *
     * return collection;
     * </pre>
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
        Database db = getDatabase();

        try
          {
            return db.queryObject(cs, externalClauses, externalClausesParameters).loadObjects(collection, cs);
          }
        finally
          {
            db.close();
          }
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
        Database db = getDatabase();

        try
          {
            db.loadAssociations(object);
          }
        finally
          {
            db.close();
          }
      }

    /**
     * Builds either an update or an insert depending on whether the object is persistent and was previously loaded/saved, or not, respectively.
     *
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.saveObject(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * @param object the object to load
     *
     * @return the number of rows updated
     *
     * @throws DatabaseException
     */
    
    public int saveObject(Object object) throws DatabaseException
      {
        return saveObject(object, null, null, (Object[])null);
      }
    
    /**
     * Builds either an update or an insert depending on whether the object is persistent and was previously loaded/saved, or not, respectively.
     * externalClauses can begin with a where clause or anything after the where clause.
     *
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.saveObject(object, externalClauses, externalClausesParameters);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * 
     * @param object the object to load
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return the number of rows updated
     *
     * @throws DatabaseException
     */
    
    public int saveObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.saveObject(object, externalClauses, externalClausesParameters);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Builds an insert
     *
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.insertObject(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * 
     * @param object the object to load
     *
     * @return the number of rows updated
     *
     * @throws DatabaseException
     * @deprecated use saveObject()
     */
    @Deprecated
    public int insertObject(Object object) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.insertObject(object);
          }
        finally
          {
            db.close();
          }
      }
    

    /**
     * Builds an update .
     *
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.updateObject(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * @param object the object to load
     *
     * @return the number of rows updated
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
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.updateObject(object, externalClauses, externalClausesParameters);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * 
     * @param object the object to load
     * @param externalClauses external clauses beginning with a where or after
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return the number of rows updated
     *
     * @throws DatabaseException
     * @deprecated use  saveObject()
     */
    @Deprecated
    public int updateObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.updateObject(object, externalClauses, externalClausesParameters);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Builds a delete statement from the object.  
     *
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.deleteObject(object);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * @param object the object to load
     *
     * @return the number of rows updated
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
     * <p>This method should not be used within a transaction manager.  Use TransactionManager methods instead.
     *
     * <p>This is a one line convenience method for:
     * <pre>
     * Database db = getDatabase();
     * 
     * try
     *   {
     *     return db.deleteObject(object, externalClauses, externalClausesParameters);
     *   }
     * finally
     *   {
     *     db.close();
     *   }
     * </pre>
     * 
     * @param object the object to load
     * @param externalClauses external clauses
     * @param externalClausesParameters the parameters to use with external clauses, can be null
     *
     * @return the number of rows updated
     *
     * @throws DatabaseException
     */
    
    public int deleteObject(Object object, String externalClauses, Object... externalClausesParameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.deleteObject(object, externalClauses, externalClausesParameters);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a SQL query. Returns a collection of Object[].
     * 
     * @param c a Collection
     * @param sql the SQL statement
     *
     * @return the Collection passed in populated with Object[]
     *
     * @throws DatabaseException
     */
    
    public Collection executeQuery(Collection c, String sql) throws DatabaseException
      {
        return executeQuery(c, false, sql);
      }
    
    /**
     * Executes a SQL query. Returns the Collection passed in populated with objects of type Class<T>.
     * 
     * @param c a Collection
     * @param cs is the class that will be associated with the result
     * @param sql the SQL statement
     *
     * @return the Collection passed in populated with objects of type Class<T>
     *
     * @throws DatabaseException
     */
    
    public <T> Collection<T> executeQuery(Collection<T> c, Class<T> cs, String sql) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            Result<T> result = db.executeQuery(cs, sql);
            
            for (T object : result)
              c.add(object);
            
            return c;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a SQL query. Returns a collection of Object[].
     * 
     * @param c a Collection
     * @param singleColumn use a single value instead of an array for a single column result
     * @param sql the SQL statement
     * @return the Collection passed in populated with Object[]
     *
     * @throws DatabaseException
     */
    
    public Collection executeQuery(Collection c, boolean singleColumn, String sql) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return ResultSetUtils.loadCollection(db.executeQuery(sql).getResultSet(), c, singleColumn);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query allows the use of '?' in 
     * SQL statements.
     * 
     * @param c a Collection
     * @param sql the SQL statement
     * @param parameters objects used to set the parameters to the query
     *
     * @return the Collection passed in
     *
     * @throws DatabaseException
     */
    
    public Collection parameterizedQuery(Collection c, String sql, Object... parameters) throws DatabaseException
      {
        return parameterizedQuery(c, false, sql);
      }
    
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query allows the use of '?' in 
     * SQL statements.
     * 
     * @param c a Collection
     * @param sql the SQL statement
     * @param parameters objects used to set the parameters to the query
     *
     * @return the Collection passed in
     *
     * @throws DatabaseException
     */
    
    public <T> Collection<T> parameterizedQuery(Collection<T> c, Class<T> cs, String sql, Object... parameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            Result<T> result = db.parameterizedQuery(cs, sql, parameters);

            for (T object : result)
              c.add(object);
            
            return c;
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a parameterized (prepared statement) query.  A parameterized query allows the use of '?' in 
     * SQL statements.
     * 
     * @param c a Collection
     * @param singleColumn use a single value instead of an array for a single column result
     * @param sql the SQL statement
     * @param parameters objects used to set the parameters to the query
     *
     * @return the Collection passed in
     *
     * @throws DatabaseException
     */
    
    public Collection parameterizedQuery(Collection c, boolean singleColumn, String sql, Object... parameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return ResultSetUtils.loadCollection(db.parameterizedQuery(sql, parameters).getResultSet(), c, singleColumn);
          }
        catch (Exception e)
          {
            throw new DatabaseException(e);
          }
        finally
          {
            db.close();
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
        Database db = getDatabase();
        
        try
          {
            return db.executeUpdate(sql);
          }
        finally
          {
            db.close();
          }
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
        Database db = getDatabase();
        
        try
          {
            return db.executeUpdate(sql, keys);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a parameterized (prepared statement) update.  A parameterized update allows the use of '?' in 
     * SQL statements.
     * 
     * @param sql the SQL statement
     * @param parameters objects used to set the parameters to the update
     *
     * @throws DatabaseException
     */
    
    public int parameterizedUpdate(String sql, Object... parameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.parameterizedUpdate(sql, parameters);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Executes a parameterized (prepared statement) update.  A parameterized update allows the use of '?' in 
     * SQL statements.
     * 
     * @param sql the SQL statement
     * @param keys is a List.  If keys is non-null, then generated keys will be returned in 
     *             the keys List.  List can also define the key columns required 
     *             (depending on the database).
     * @param parameters objects used to set the parameters to the update
     *
     * @throws DatabaseException
     */
    
    public int parameterizedUpdate(String sql, List keys, Object... parameters) throws DatabaseException
      {
        Database db = getDatabase();
        
        try
          {
            return db.parameterizedUpdate(sql, keys, parameters);
          }
        finally
          {
            db.close();
          }
      }
    
    /**
     * Returns a DatabaseManager local instance of PersistentClassManager used 
     * to associate information regarding persistence preferences for a Class 
     * whose objects will be involved with EJP persistence methods.  This 
     * version wont bleed thru to other applications running under the same 
     * JVM.  Calling this method even once, will override the global 
     * PersistentClassManager and all DatabaseManager, Database, Result methods 
     * will call upon this PersistentClassManager for persistent class preferences.
     * 
     * @return returns a DatabaseManager local PersistentClassManager
     */
    public DatabaseManager.PersistentClassManager getPersistentClassManager()
      {
        if (persistentClassManager.classMap == null)
          persistentClassManager.init();
        
        return persistentClassManager;
      }

    /**
     * This class is used to associate information regarding persistence preferences
     * for a Class whose objects will be involved with EJP persistence methods.  This 
     * class provides a DatabaseManager local version of the Singleton (global 
     * static) oriented PersistentClassManager.  This version wont bleed thru 
     * to other applications running under the same JVM.
     */

    public static class PersistentClassManager
      {
        Map<Class, ClassInformation> classMap = null;
        private Boolean defaultReloadAfterSave = true, defaultIgnoreAssociations = false, defaultLazyLoading = true;

        void init() 
          {
            if (classMap == null)
              classMap = Collections.synchronizedMap(new HashMap<Class, ClassInformation>());
          }
        
        /**
         * Define the table to use with this class. Overrides the default table search.
         *
         * @param cs the class of the persistent object to affect
         * @param tableMapping the table name as it exists in the database
         */

        public void setTableMapping(Class cs, String tableMapping) 
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

        public void setColumnMapping(Class cs, Map<String, String> columnMapping)
          {
            logger.debug("columnMapping = {}", columnMapping);

            get(cs).columnMapping = columnMapping;
          }

        /**
         * Define whether or not to load associations.
         *
         * @param cs the class of the persistent object to affect
         * @param ignoreAssociations ignore associations if true
         */

        public void setIgnoreAssociations(Class cs, Boolean ignoreAssociations)
          {
            logger.debug("ignoreAssociations = {}", ignoreAssociations);

            get(cs).ignoreAssociations = ignoreAssociations;
          }

        /**
         * Define the default for whether or not to load associations.
         *
         * @param ignoreAssociations ignore associations if true
         */

        public void setDefaultIgnoreAssociations(Boolean defaultIgnoreAssociations)
          {
            logger.debug("ignoreAssociations = {}", defaultIgnoreAssociations);

            this.defaultIgnoreAssociations = defaultIgnoreAssociations;
          }

        /**
         * Define whether or not to lazy load associations.
         *
         * @param cs
         * @param lazyLoading 
         */

        public void setLazyLoading(Class cs, Boolean lazyLoading)
          {
            logger.debug("ignoreAssociations = {}", lazyLoading);

            get(cs).lazyLoading = lazyLoading;
          }

        /**
         * Define whether or not to lazy load associations.
         *
         * @param lazyLoading 
         */

        public void setDefaultLazyLoading(Boolean defaultLazyLoading)
          {
            logger.debug("ignoreAssociations = {}", defaultLazyLoading);

            this.defaultLazyLoading = defaultLazyLoading;
          }

        /**
         * Define a set of property names (method name without get/set) to include in generated SQL statements when null.
         *
         * @param nullValuesToIncludeInQueries a variable set of property names to include in generated SQL statements when null
         */
        public void setNullValuesToIncludeInQueries(Class cs, String ... nullValuesToIncludeInQueries)
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
        public void clearNullValuesToIncludeInQueries(Class cs) 
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
        public void setNullValuesToIncludeInSaves(Class cs, String ... nullValuesToIncludeInSaves)
          {
            logger.debug("nullValuesToIncludeInSaves = {}", nullValuesToIncludeInSaves);

            Set<String> nullValues = new HashSet<String>();

            for (String value : nullValuesToIncludeInSaves)
              nullValues.add(Character.toLowerCase(value.charAt(0)) + value.substring(1));

            //get(cs).nullValuesToIncludeInSaves = nullValues;
          }

        /**
         * Clear the set of previously defined property names (method name without get/set) to include in generated SQL statements when null.
         */
        @Deprecated
        public void clearNullValuesToIncludeInSaves(Class cs) 
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

        public void setReloadAfterSave(Class cs, Boolean reloadAfterSave) 
          {
            logger.debug("reloadAfterSave = {}", reloadAfterSave);

            get(cs).reloadAfterSave = reloadAfterSave;
          }

        /**
         * Set the default, for all objects, for reload after save.
         * 
         * @param reloadAfterSave
         */

        public void setDefaultReloadAfterSave(Boolean reloadAfterSave) 
          {
            logger.debug("defaultReloadAfterSave = {}", reloadAfterSave);

            defaultReloadAfterSave = reloadAfterSave;
          }

        /**
         * Remove class from the persistent class manager (has the effect of resetting the class to new).
         *
         * @param cs the class of the persistent object to affect
         */
        public void remove(Class cs) 
          {
            logger.debug("cs = {}", cs);

            classMap.remove(cs);
          }

        String getColumnMapping(Class cs, String propertyName)
          {
            Map<String, String> columnMapping = get(cs).columnMapping;
            String columnName = columnMapping.get(propertyName);

            if (columnName == null)
              columnName = columnMapping.get(Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1));

            if (columnName == null)
              columnName = columnMapping.get(propertyName.toLowerCase());

            return columnName;
          }

        String getReverseColumnMapping(Class cs, String propertyName, boolean buildMapping)
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

        ClassInformation get(Class cs)
          {
            if (classMap == null)
              return ejp.PersistentClassManager.get(cs);
            
            ClassInformation p = classMap.get(cs);

            if (p == null)
              classMap.put(cs, p = new ClassInformation(defaultReloadAfterSave, defaultIgnoreAssociations, defaultLazyLoading));

            return p;
          }
      }
  }
