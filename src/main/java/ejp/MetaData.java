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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import ejp.utilities.StringUtils;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ejp.utilities.SimpleInflector;

/**
 * This class provides database level metadata.
 */

@SuppressWarnings("unchecked")
public final class MetaData
  {
    public static final int STORES_UNKNOWN = 0;
    public static final int STORES_UPPERCASE = 1;
    public static final int STORES_LOWERCASE = 2;
    public static final int STORES_MIXEDCASE = 3;
    
    private static Logger logger = LoggerFactory.getLogger(MetaData.class);
    private static ConcurrentHashMap metaDataMap = new ConcurrentHashMap();
    
    private String tableTypes[] = new String[] { "TABLE", "VIEW" }, identifierQuoteString = "", 
                   searchStringEscape = "", catalogSeparator = "", databaseUrl;
    private ConcurrentHashMap tables = new ConcurrentHashMap(), tableCache = new ConcurrentHashMap();
    private Set stripTablePrefixes, stripTableSuffixes, stripColumnPrefixes, stripColumnSuffixes;
    private boolean supportsGeneratedKeys, supportsSavepoints, supportsBatchUpdates, strictClassTableMatching = false, strictMethodColumnMatching = true;
    private int storesCase = 0;

    static MetaData getMetaData(Connection connection) throws SQLException, DatabaseException
      {
        String databaseUrl = connection.getMetaData().getURL();
        MetaData metaData = (MetaData)metaDataMap.get(databaseUrl);

        if (metaData != null)
          return metaData;
        
        return loadMetaData(connection);
      }
    
    static MetaData loadMetaData(Connection connection) throws SQLException, DatabaseException
      {
        String databaseUrl = connection.getMetaData().getURL();
        MetaData metaData = (MetaData)metaDataMap.get(databaseUrl);

        if (metaData == null)
          {
            metaData = new MetaData();
            DatabaseMetaData dbMetaData = connection.getMetaData();

            try
              {
                logger.debug("database product name = {}", dbMetaData.getDatabaseProductName());
                logger.debug("database product version = {}", dbMetaData.getDatabaseProductVersion());
                logger.debug("database version = {}.{}", dbMetaData.getDatabaseMajorVersion(), dbMetaData.getDatabaseMinorVersion());
                logger.debug("JDBC driver version = {}.{}", dbMetaData.getDriverMajorVersion(), dbMetaData.getDriverMinorVersion());
                logger.debug("user name = {}", dbMetaData.getUserName());
                logger.debug("supports transactions = {}", dbMetaData.supportsTransactions());
                logger.debug("supports multiple transactions = {}", dbMetaData.supportsMultipleTransactions());
                logger.debug("supports transaction isolation level TRANSACTION_READ_COMMITTED = {}", dbMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
                logger.debug("supports transaction isolation level TRANSACTION_READ_UNCOMMITTED = {}", dbMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
                logger.debug("supports transaction isolation level TRANSACTION_REPEATABLE_READ = {}", dbMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
                logger.debug("supports transaction isolation level TRANSACTION_SERIALIZABLE = {}", dbMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
                logger.debug("supports result set TYPE_FORWARD_ONLY = {}", dbMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
                logger.debug("supports result set TYPE_SCROLL_INSENSITIVE = {}", dbMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
                logger.debug("supports result set TYPE_SCROLL_SENSITIVE = {}", dbMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
                logger.debug("supports result set holdability CLOSE_CURSORS_AT_COMMIT = {}", dbMetaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
                logger.debug("supports result set holdability HOLD_CURSORS_OVER_COMMIT = {}", dbMetaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
                logger.debug("stores lower case identifiers = {}", dbMetaData.storesLowerCaseIdentifiers());
                logger.debug("stores lower case quoted identifiers = {}", dbMetaData.storesLowerCaseQuotedIdentifiers());
                logger.debug("stores upper case identifiers = {}", dbMetaData.storesUpperCaseIdentifiers());
                logger.debug("stores upper case quoted identifiers = {}", dbMetaData.storesUpperCaseQuotedIdentifiers());
                logger.debug("stores mixed case identifiers = {}", dbMetaData.storesMixedCaseIdentifiers());
                logger.debug("stores mixed case quoted identifiers = {}", dbMetaData.storesMixedCaseQuotedIdentifiers());
              }
            catch (Exception e)
              {
                logger.error(e.getMessage(),e);
              }
            
            logger.debug("Catalog term = {}", dbMetaData.getCatalogTerm());
            logger.debug("Schema term = {}", dbMetaData.getSchemaTerm());
            
            try
              {
                if (dbMetaData.supportsSavepoints())
                  {
                    Savepoint savepoint = connection.setSavepoint();
                    connection.releaseSavepoint(savepoint);
                  }
                
                metaData.supportsSavepoints = dbMetaData.supportsSavepoints();
              }
            catch (Exception e)
              {
                logger.warn("The database metadata reports it supports savepoints, but the database fails with setSavepoint().  Therefore, the database probably does not support savepoints");
              }
            
            logger.debug("supports savepoints = {}", metaData.supportsSavepoints);
            
            metaData.supportsBatchUpdates = dbMetaData.supportsBatchUpdates();
            
            if (dbMetaData.storesLowerCaseIdentifiers() || dbMetaData.storesLowerCaseQuotedIdentifiers())
              metaData.storesCase = STORES_LOWERCASE;
            else if (dbMetaData.storesUpperCaseIdentifiers() || dbMetaData.storesUpperCaseQuotedIdentifiers())
              metaData.storesCase = STORES_UPPERCASE;
            else if (dbMetaData.storesMixedCaseIdentifiers() || dbMetaData.storesMixedCaseQuotedIdentifiers())
              metaData.storesCase = STORES_MIXEDCASE;

            logger.debug("maximum concurrent connections = {}", dbMetaData.getMaxConnections());
            
            metaData.identifierQuoteString = dbMetaData.getIdentifierQuoteString();
            
            if (metaData.identifierQuoteString.equals(" "))
              metaData.identifierQuoteString = "";
              
            logger.debug("identifier quote string = '{}'", metaData.identifierQuoteString);
            logger.debug("catalog separator = {}", (metaData.catalogSeparator = dbMetaData.getCatalogSeparator()));
            logger.debug("supports generated keys = {}", (metaData.supportsGeneratedKeys = dbMetaData.supportsGetGeneratedKeys()));
            logger.debug("search string escape = {}", (metaData.searchStringEscape = dbMetaData.getSearchStringEscape()));
            logger.debug("database url = {}", (metaData.databaseUrl = databaseUrl));

            if (metaDataMap.putIfAbsent(databaseUrl, metaData) != null)
              metaData = (MetaData)metaDataMap.get(databaseUrl);
          }

        return metaData;
      }

    /**
     * Returns the identifier quote string ("'", etc).
     * 
     * @return the identifier quote string ("'", etc).
     */
    public String getIdentifierQuoteString() { return identifierQuoteString; }

    /**
     * Returns the search string escape string.
     * 
     * @return the search string escape string.
     */
    public String getSearchStringEscape() { return searchStringEscape; }

    /**
     * Returns the JDBC URL.
     * 
     * @return the JDBC URL
     */
    public String getDatabaseUrl() { return databaseUrl; }

    /**
     * Returns the method for storing case with table names and column names.
     * 
     * @return one of STORES_UNKNOWN, STORES_UPPERCASE, STORES_LOWERCASE, STORES_MIXEDCASE
     */
    public int getStoresCase() { return storesCase; }

    /**
     * Returns true if the JDBC driver supports generated keys via parameters 
     * to one of the various update methods.
     * 
     * @return true if the JDBC driver supports generated keys
     */
    public boolean supportsGeneratedKeys() { return supportsGeneratedKeys; }

    /**
     * Returns true if the JDBC driver supports save points.
     * @return true if the JDBC driver supports save points
     */
    public boolean supportsSavepoints() { return supportsSavepoints; }

    /**
     * Returns true if the JDBC driver supports batch updates.
     * @return true if the JDBC driver supports batch updates
     */
    public boolean supportsBatchUpdates() { return supportsBatchUpdates; }

    /**
     * Set the table types to search for (default is "TABLE" and "VIEW").
     * @param tableTypes an array of table types (see JDBC javadoc)
     */
    public void setTableTypes(String[] tableTypes)
      {
        this.tableTypes = tableTypes;
      }

    /**
     * Use strict method/column matching (default is true).  If true method 
     * property names and column names have to match exactly (less camelcase 
     * and underlines).  If false the method name can exist in the column name
     * as a substring.
     * 
     * @param trueFalse true to use strict method/column matching
     */
    public void setStrictMethodColumnMatching(boolean trueFalse) { strictMethodColumnMatching = trueFalse; }

    boolean isStrictMethodColumnMatching() { return strictMethodColumnMatching; }

    /**
     * Use strict class/table name matching (default is false).  If true class 
     * names and table names have to match exactly (less camelcase 
     * and underlines).  If false the class name can exist in the table name
     * as a substring.  Several steps are performed in trying to match class 
     * names to table names.
     * 
     * @param trueFalse true to use strict class/table matching
     */
    public void setStrictClassTableMatching(boolean trueFalse) { strictClassTableMatching = trueFalse; }
    
    /**
     * Set of prefixes to be stripped from table names to help in class to table name matching.
     * @param stripTablePrefixes a set of prefixes (Strings)
     */
    public void setTablePrefixesToStrip(Set stripTablePrefixes) { this.stripTablePrefixes = stripTablePrefixes; }

    /**
     * Set of suffixes to be stripped from table names to help in class to table name matching.
     * @param stripTableSuffixes a set of suffixes (Strings)
     */
    public void setTableSuffixesToStrip(Set stripTableSuffixes) { this.stripTableSuffixes = stripTableSuffixes; }

    /**
     * Set of prefixes to be stripped from column names to help in method to column name matching.
     * @param stripColumnPrefixes a set of prefixes (Strings)
     */
    public void setColumnPrefixesToStrip(Set stripColumnPrefixes) { this.stripColumnPrefixes = stripColumnPrefixes; }

    /**
     * Set of suffixes to be stripped from column names to help in method to column name matching.
     * @param stripColumnSuffixes a set of suffixes (Strings)
     */
    public void setColumnSuffixesToStrip(Set stripColumnSuffixes) { this.stripColumnSuffixes = stripColumnSuffixes; }
        
    /**
     * Returns the table metadata for a given table name.
     * 
     * @param connection JDBC connection
     * @param catalogPattern the catalog (can be null)
     * @param schemaPattern the schema (can be null)
     * @param tableName the table name to search for
     * @param objectClass object class that can have added annotations and/or interfaces
     * 
     * @return a Table metadata instance
     * 
     * @throws java.sql.SQLException
     * @throws ejp.DatabaseException
     */
    public Table getTable(Database db, String tableName, Class objectClass) throws SQLException, DatabaseException
      {
        String searchName = (db.getCatalogPattern() != null ? db.getCatalogPattern() + catalogSeparator : "")
                          + (db.getSchemaPattern() != null ? db.getSchemaPattern() + "." : "")
                          + tableName;
        Table table = (Table)tableCache.get(searchName);

        if (table == null)
          {
            synchronized(objectClass)
              {
                if ((table = (Table)tableCache.get(searchName)) == null)
                  {
                    if ((table = tableSearch(db, tableName, objectClass, true)) == null)
                      table = tableSearch(db, tableName, objectClass, false);

                    if (!(table instanceof NullTable) && !table.isTableDetailLoaded())
                      loadTableDetail(db, table);
                  }
              }
          }
        
        if (table instanceof NullTable)
          return null;
        else
          return table;
      }
    
    /* load all possiblities and scan for an exact match, or a single match */
    Table tableSearch(Database db, String tableName, Class objectClass, boolean exactMatch) throws SQLException, DatabaseException
      {
        String searchName = (db.getCatalogPattern() != null ? db.getCatalogPattern() + catalogSeparator : "")
                          + (db.getSchemaPattern() != null ? db.getSchemaPattern() + "." : "")
                          + tableName;
        Table table = (Table)tableCache.get(searchName);
        
        // table already loaded
        if (table != null)
          return table;
        
        String name = null;

        logger.debug("Searching for table {}", tableName);

        // search and load table mapping (override with strict/exact matching)
        if (table == null && (name = db.getPersistentClassManager().get(objectClass).tableMapping) != null)
          {
            tableName = name;
            exactMatch = true;
            table = loadTables(db, db.getCatalogPattern(), db.getSchemaPattern(), name, exactMatch);
          }

        // search and load plurals and PLURALS
        if (table == null)
          {
            String[] plurals = SimpleInflector.pluralize(tableName);

            for (int i = 0; table == null && i < plurals.length; i++)
              table = loadTables(db, db.getCatalogPattern(), db.getSchemaPattern(), name = plurals[i], exactMatch);
          }
        
        // search and load for tablename and TABLENAME
        if (table == null)
          table = loadTables(db, db.getCatalogPattern(), db.getSchemaPattern(), name = tableName, exactMatch);
        
        // search and load for table_name and TABLE_NAME
        if (table == null)
          {
            name = StringUtils.camelCaseToLowerCaseUnderline(tableName);
            table = loadTables(db, db.getCatalogPattern(), db.getSchemaPattern(), name, exactMatch);
          }
        
        // scan for possible matches (indexOf)
        if (table == null && !exactMatch && !strictClassTableMatching)
          table = tableScan(db.getCatalogPattern(), db.getSchemaPattern(), name, false);

        if (table != null)
          {
            if (tableCache.putIfAbsent(searchName, table) != null)
              table = (Table)tableCache.get(searchName);
          }
        else if (!exactMatch)
          {
            tableCache.put(searchName, table = new NullTable());

            logger.debug("Table {} not found!", tableName);
          }

        return table;
      }

    Table loadTables(Database db, String catalogPattern, String schemaPattern, String tablePattern, boolean exactMatch) throws SQLException, DatabaseException
      {
        DatabaseMetaData metaData = db.getConnection().getMetaData();
        boolean tableFound = false;
        Table table = null;
        String searchName = null;
        
        for (int characterCase = 0; characterCase < 2 && !tableFound; characterCase++)
          {
            if (characterCase > 0)
              {
                tablePattern = characterCase == 1 ? tablePattern.toUpperCase() : tablePattern.toLowerCase();

                if (catalogPattern != null)
                  catalogPattern = characterCase == 1 ? catalogPattern.toUpperCase() : catalogPattern.toLowerCase();

                if (schemaPattern != null)
                schemaPattern = characterCase == 1 ? schemaPattern.toUpperCase() : schemaPattern.toLowerCase();
              }
            
            ResultSet resultSet = metaData.getTables(catalogPattern, schemaPattern, 
                                                     exactMatch ? tablePattern : "%" + tablePattern + "%", 
                                                     tableTypes);

            while (resultSet.next() && !tableFound)
              {
                table = new Table(resultSet.getString("table_name"),
                                        resultSet.getString("table_cat"), catalogPattern,
                                        resultSet.getString("table_schem"), schemaPattern,
                                        resultSet.getString("table_type"));

                searchName = normalizeName(table.getAbsoluteTableName(false));

                tables.putIfAbsent(searchName, table);

                logger.debug("Found table: {}", table);
                
                if ((table = tableScan(db.getCatalogPattern(), db.getSchemaPattern(), tablePattern, true)) != null)
                  tableFound = true;
              }

            resultSet.close();
          }
        
        return table;
      }

    Table tableScan(String catalogName, String schemaName, String tableName, boolean strictMatch) throws DatabaseException 
      {
        String searchName = normalizeName((catalogName != null ? catalogName + catalogSeparator : "")
                          + (schemaName != null ? schemaName + "." : "")
                          + tableName);
        Table table = (Table)tables.get(searchName);
       
        if (table != null)
          return table;

        tableName = normalizeName(tableName);
        
        catalogName = catalogName != null ? catalogName.toLowerCase() : "";
        schemaName = schemaName != null ? schemaName.toLowerCase() : "";

        Iterator it = tables.entrySet().iterator();
        Table itTable;
        String matchName1, matchName2, itTableCatalog, itTableSchema;

        while (it.hasNext())
          {
            itTable = (Table)((Map.Entry)it.next()).getValue();
            matchName1 = normalizeName(itTable.getTableName());
            matchName2 = null;
            itTableCatalog = itTable.getCatalogName() != null ? itTable.getCatalogName().toLowerCase() : "";
            itTableSchema = itTable.getSchemaName() != null ? itTable.getSchemaName().toLowerCase() : "";

            if (stripTablePrefixes != null || stripTableSuffixes != null)
              matchName2 = normalizeName(stripTableName(itTable.getTableName()));

            if ((tableName.equals(matchName1) || (matchName2 != null && tableName.equals(matchName2)))
                      && (catalogName.length() == 0 || catalogName.equals(itTableCatalog))
                      && (schemaName.length() == 0 || schemaName.equals(itTableSchema)))
              {
                if (table == null)
                  table = itTable;
                else
                  throw new DatabaseException("Scanning produces multiple possible tables for table name '" + tableName + "'\n"
                                            + "To obtain an exact match you can further qualify the naming, or add catalog/schema qualifiers,\n or define prefix/suffix stripping, or table name mapping.");
              }
          }

        if (!strictMatch && table == null)
          {
            it = tables.entrySet().iterator();
            
            while (it.hasNext())
              {
                itTable = (Table)((Map.Entry)it.next()).getValue();
                matchName1 = normalizeName(itTable.getTableName());
                itTableCatalog = itTable.getCatalogName() != null ? itTable.getCatalogName().toLowerCase() : "";
                itTableSchema = itTable.getSchemaName() != null ? itTable.getSchemaName().toLowerCase() : "";
                
                if (matchName1.indexOf(tableName) != -1
                          && (catalogName.length() == 0 || catalogName.equals(itTableCatalog))
                          && (schemaName.length() == 0 || schemaName.equals(itTableSchema)))
                  {
                    if (table == null || matchName1.equals(tableName))
                      table = itTable;
                    else
                      throw new DatabaseException("Scanning produces multiple possible tables for table name '" + tableName + "'\n"
                                                + "To obtain an exact match you can further qualify the naming, or add catalog/schema qualifiers,\n or define prefix/suffix stripping, or table name mapping.");
                  }
              }
          }
        
        return table;
      }

    void loadTableDetail(Database db, Table table) throws SQLException, DatabaseException
      {
        Statement statement = db.getConnection().createStatement();
        DatabaseMetaData metaData = db.getConnection().getMetaData();
        ResultSet resultSet = metaData.getPrimaryKeys(table.getCatalogName(), table.getSchemaName(), table.getTableName());

        Map primaryKeys = new HashMap();
        String name;

        while (resultSet.next())
          {
            name = resultSet.getString("column_name");
            primaryKeys.put(name, table.new Key(name, resultSet.getString("table_name"), resultSet.getString("table_cat"), resultSet.getString("table_schem")));
          }

        table.setPrimaryKeys(primaryKeys);

        resultSet.close();

        resultSet = metaData.getBestRowIdentifier(table.getCatalogName(), table.getSchemaName(), table.getTableName(), DatabaseMetaData.bestRowSession, true);

        Set bestRowIds = new HashSet();

        while (resultSet.next())
          bestRowIds.add(resultSet.getString("column_name"));

        table.setBestRowIds(bestRowIds);

        resultSet.close();

        resultSet = metaData.getImportedKeys(table.getCatalogName(), table.getSchemaName(), table.getTableName());

        Map importedKeys = new HashMap();

        while (resultSet.next())
          {
            name = resultSet.getString("fkcolumn_name");
            importedKeys.put(name, table.new Key(name, resultSet.getString("fktable_cat"), resultSet.getString("fktable_schem"), resultSet.getString("fktable_name"),
                             resultSet.getString("pkcolumn_name"), resultSet.getString("pktable_cat"), resultSet.getString("pktable_schem"), resultSet.getString("pktable_name")));
          }

        table.setImportedKeys(importedKeys);

        resultSet.close();

        resultSet = metaData.getExportedKeys(table.getCatalogName(), table.getSchemaName(), table.getTableName());

        Map exportedKeys = new HashMap();

        while (resultSet.next())
          {
            name = resultSet.getString("pkcolumn_name");
            exportedKeys.put(name, table.new Key(name, resultSet.getString("pktable_cat"), resultSet.getString("pktable_schem"), resultSet.getString("pktable_name"),
                             resultSet.getString("fkcolumn_name"), resultSet.getString("fktable_cat"), resultSet.getString("fktable_schem"), resultSet.getString("fktable_name")));
          }

        table.setExportedKeys(exportedKeys);

        resultSet.close();

        resultSet = metaData.getColumns(table.getCatalogName(), table.getSchemaName(), table.getTableName(), null);

        Map columns = new HashMap();
        String columnName = null;
        Table.Column column = null;

        while (resultSet.next())
          {
            columnName = resultSet.getString("COLUMN_NAME");

            column = table.new Column(columnName,
                                      resultSet.getString("TYPE_NAME"),
                                      resultSet.getInt("DATA_TYPE"),
                                      resultSet.getInt("COLUMN_SIZE"),
                                      resultSet.getInt("DECIMAL_DIGITS"),
                                      resultSet.getInt("NUM_PREC_RADIX"),
                                      resultSet.getString("IS_NULLABLE").equalsIgnoreCase("Yes") ? true : false,
                                      primaryKeys.get(columnName) != null,
                                      bestRowIds.contains(columnName));

            columns.put(normalizeName(columnName), column);
          }

        table.setColumns(columns);

        resultSet.close();

        if ((resultSet = statement.executeQuery("select * from " + table.getAbsoluteTableName(true) + " where 1 = 0")) != null)
          {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            if (resultSetMetaData != null)
              for (int i = 0; i < resultSetMetaData.getColumnCount(); i++)
                {
                  column = table.getColumn(normalizeName(resultSetMetaData.getColumnName(i+1)));

                  if (column != null)
                    {
                      column.setAdditionalInfo(resultSetMetaData.getColumnLabel(i+1),
                                               resultSetMetaData.getColumnClassName(i+1),
                                               resultSetMetaData.isAutoIncrement(i+1),
                                               resultSetMetaData.isReadOnly(i+1),
                                               resultSetMetaData.isSearchable(i+1));

                      if (column.isAutoIncrement())
                        table.setGeneratedKey(column.getColumnName());
                    }
                }

            resultSet.close();
          }
        
        table.setTableDetailLoaded(true);
      }
    
    /**
     * The main table metadata.
     */
    public class Table
      {
        private String tableName, catalogName, catalogPattern, schemaName, schemaPattern, type, generatedKey;
        private ConcurrentHashMap columnCache = new ConcurrentHashMap();
        private Map<String, Key> exportedKeys, importedKeys;
        private Map<String, String> primaryKeys;
        private Map<String, Column> columns;
        private boolean isTableDetailLoaded;
        private Set bestRowIds;

        Table() { }
        Table(String tableName, String catalogName, String catalogPattern, String schemaName, String schemaPattern, String type)
          {
            this.tableName = tableName;
            this.catalogName = catalogName;
            this.catalogPattern = catalogPattern;
            this.schemaName = schemaName;
            this.schemaPattern = schemaPattern;
            this.type = type;
          }

        public String getAbsoluteTableName(boolean useQuotes) 
          {
            String quoteString = useQuotes ? identifierQuoteString : "";
            
            return (catalogName != null && catalogPattern != null ? identifierQuoteString + catalogName + identifierQuoteString + catalogSeparator : "")
                 + (schemaName != null && schemaPattern != null ? identifierQuoteString + schemaName + identifierQuoteString + "." : "")
                 + identifierQuoteString + tableName + identifierQuoteString;
          }
        
        /**
         * Returns the table name.
         * 
         * @return the table name
         */
        public String getTableName() { return tableName; }
        
        /**
         * Returns the catalog name.
         * 
         * @return the catalog name
         */
        public String getCatalogName() { return catalogName; }
        
        /**
         * Returns the schema name.
         * 
         * @return the schema name
         */
        public String getSchemaName() { return schemaName; }
        
        /**
         * The type of table as defined by setTableTypes().
         * 
         * @return a valid table type name.
         */
        public String getType() { return type; }

        public String toString() { return "catalog = " + catalogName + ", schema = " + schemaName + ", table = " + tableName; }
        
        boolean isTableDetailLoaded() { return isTableDetailLoaded; }
        void setTableDetailLoaded(boolean isTableDetailLoaded) { this.isTableDetailLoaded = isTableDetailLoaded; }
        
        /**
         * May return a set of columns that can best be used for row identifiers.
         * 
         * @return a set of columns that can best be used for row identifiers.
         */
        public Set getBestRowIds() { return bestRowIds; }
        void setBestRowIds(Set bestRowIds) { this.bestRowIds = bestRowIds; }

        /**
         * Returns the column representing a generated key.
         * 
         * @return the column representing a generated key
         */
        public String getGeneratedKey() { return generatedKey; }
        void setGeneratedKey(String generatedKey) { this.generatedKey = generatedKey; }
        
        void setColumns(Map columns)
          {
            this.columns = columns;
          }
        
        /**
         * Returns the column representing a generated key.
         * 
         * @return the column representing a generated key
         */
        public String getPossibleGeneratedKey() 
          {
            if (generatedKey != null)
              return generatedKey;

            Column lastColumnMatch = null;
            String key = null;
            Column column = null;
            
            for (Iterator it = primaryKeys.keySet().iterator(); it.hasNext();)
              {
                key = (String)it.next();
                column = (Column)columns.get(normalizeName(key));
                
                if (importedKeys.get(key) == null)
                  lastColumnMatch = column;
              }
            
            if (lastColumnMatch != null)
              return lastColumnMatch.getColumnName();
            
            return null;
          }

        /**
         * Returns a map of Table.Key instances representing the primary key(s).
         * 
         * @return a map of Table.Key instances representing the primary key(s)
         */
        public Map<String, String> getPrimaryKeys() { return primaryKeys; }
        void setPrimaryKeys(Map<String, String> primaryKeys) { this.primaryKeys = primaryKeys; }
        
        /**
         * Returns a map of Table.Key instances representing the exported key(s).
         * 
         * @return a map of Table.Key instances representing the exported key(s)
         */
        public Map<String, Key> getExportedKeys() { return exportedKeys; }
        void setExportedKeys(Map<String, Key> exportedKeys) { this.exportedKeys = exportedKeys; }
        
        /**
         * Returns a map of Table.Key instances representing the imported key(s).
         * 
         * @return a map of Table.Key instances representing the imported key(s)
         */
        public Map<String, Key> getImportedKeys() { return importedKeys; }
        void setImportedKeys(Map<String, Key> importedKeys) { this.importedKeys = importedKeys; }

        /**
         * Returns a Table.Column instance.
         * 
         * @param columnName the column to return
         * 
         * @return a Table.Column instance
         * 
         * @throws ejp.DatabaseException
         */
        public Column getColumn(String columnName) throws DatabaseException
          {
            return (Column)columns.get(normalizeName(columnName));
          }
        
        /**
         * Returns a Table.Column instance.
         * 
         * @param columnName the column to return
         * @param objectClass a class that may have added annotations and/or interfaces
         * 
         * @return a Table.Column instance
         * 
         * @throws ejp.DatabaseException
         */
        public Column getColumn(Database db, String columnName, Class objectClass) throws DatabaseException
          {
            Column column = (Column)columnCache.get(columnName);
            
            if (column != null)
              {
                if (column instanceof NullColumn)
                  return null;

                return column;
              }
            
            return columnSearch(db, columnName, objectClass);
          }
        
        Column columnSearch(Database db, String columnName, Class objectClass) throws DatabaseException
          {
            String normalizedColumnName = normalizeName(columnName), name = null;
            Column column = null;

            if ((name = db.getPersistentClassManager().getColumnMapping(objectClass, columnName)) != null)
              {
                column = (Column)columns.get(normalizeName(name));
              }
            else 
              {
                column = (Column)columns.get(normalizedColumnName);
              }
              
            if (column == null && (stripColumnPrefixes != null || stripColumnSuffixes != null))
              {
                Iterator it = columns.entrySet().iterator();
                Column itColumn = null;
                String strippedName = null;

                while (it.hasNext())
                  {
                    itColumn = (Column)((Map.Entry)it.next()).getValue();
                    strippedName = normalizeName(stripColumnName(itColumn.getColumnName()));

                    if (columnName.equals(strippedName))
                      {
                        if (column == null)
                          column = itColumn;
                        else
                          throw new DatabaseException("Scanning produces multiple possible columns for column name '" + columnName + "' found in table '" + getAbsoluteTableName(false) + "'\n"
                                                    + "To obtain an exact match you can further qualify the naming, or define prefix/suffix stripping, or column name mapping.");
                      }
                  }
              }

            if (column == null && !strictMethodColumnMatching)
              {
                Iterator it = columns.keySet().iterator();

                while (it.hasNext())
                  {
                    name = (String)it.next();

                    if (name.indexOf(normalizedColumnName) != -1)
                      {
                        if (column == null || name.equals(normalizedColumnName))
                          column = (Column)columns.get(name);
                        else
                          throw new DatabaseException("Scanning produces multiple possible columns for column name '" + columnName + "' found in table '" + getAbsoluteTableName(false) + "'\n"
                                                    + "To obtain an exact match you can further qualify the naming, or define prefix/suffix stripping, or column name mapping.");
                      }
                  }
              }

            if (column == null)
              column = new NullColumn();
            
            if (columnCache.putIfAbsent(columnName, column) != null)
              column = (Column)columnCache.get(columnName);

            if (column instanceof NullColumn)
              {
                logger.debug("Column {} not matched or ignored!", columnName);
                
                return null;
              }
            
            return column;
          }

        /**
         * The Table.Column metadata.
         */
        public class Column
          {
            private String columnName, columnLabel, typeName, className;
            private int dataType, columnSize, decimalDigits, radix;
            private boolean isNullable, isPrimaryKey, isRowId, isAutoIncrement, isReadOnly, isSearchable;

            Column() {}
            Column(String columnName, String typeName, int dataType, int columnSize, int decimalDigits, int radix, boolean isNullable, boolean isPrimaryKey, boolean isRowId)
              {
                this.columnName = columnName;
                this.typeName = typeName;
                this.dataType = dataType;
                this.columnSize = columnSize;
                this.decimalDigits = decimalDigits;
                this.radix = radix;
                this.isNullable = isNullable;
                this.isPrimaryKey = isPrimaryKey;
                this.isRowId = isRowId;
              }

            void setAdditionalInfo(String columnLabel, String className, boolean isAutoIncrement, boolean isReadOnly, boolean isSearchable)
              {
                this.columnLabel = columnLabel;
                this.className = className;
                this.isAutoIncrement = isAutoIncrement;
                //this.isReadOnly = isReadOnly;
                this.isSearchable = isSearchable;
              }
            
            public String getColumnName() { return columnName; }
            public String getColumnLabel() { return columnLabel; }
            public String getClassName() { return className; }
            public String getTypeName() { return typeName; }
            public int getDataType() { return dataType; }
            public int getColumnSize() { return columnSize; }
            public int getDecimalDigits() { return decimalDigits; }
            public int getRadix() { return radix; }
            public boolean isNullable() { return isNullable; }
            public boolean isPrimaryKey() { return isPrimaryKey; }
            public boolean isRowId() { return isRowId; }
            public boolean isAutoIncrement() { return isAutoIncrement; }
            public boolean isReadOnly() { return isReadOnly || isAutoIncrement; }
            public boolean isSearchable() { return isSearchable; }
          }
        
        class NullColumn extends Column { }

        public class Key
          {
            private String localColumnName, localTableCatalog, localTableSchema, localTableName,
                           foreignColumnName, foreignTableCatalog, foreignTableSchema, foreignTableName;

            Key(String localColumnName, String localTableCatalog, String localTableSchema, String localTableName)
              {
                this.localColumnName = localColumnName;
                this.localTableCatalog = localTableCatalog;
                this.localTableSchema = localTableSchema;
                this.localTableName = localTableName;
              }
            
            Key(String localColumnName, String localTableCatalog, String localTableSchema, String localTableName,
                String foreignColumnName, String foreignTableCatalog, String foreignTableSchema, String foreignTableName)
              {
                this.localColumnName = localColumnName;
                this.localTableCatalog = localTableCatalog;
                this.localTableSchema = localTableSchema;
                this.localTableName = localTableName;
                this.foreignColumnName = foreignColumnName;
                this.foreignTableCatalog = foreignTableCatalog;
                this.foreignTableSchema = foreignTableSchema;
                this.foreignTableName = foreignTableName;
              }
            
            public String getForeignColumnName() { return foreignColumnName; }
            public String getForeignTableCatalog() { return foreignTableCatalog; }
            public String getForeignTableSchema() { return foreignTableSchema; }
            public String getForeignTableName() { return foreignTableName; }
            public String getLocalColumnName() { return localColumnName; }
            public String getLocalTableCatalog() { return localTableCatalog; }
            public String getLocalTableSchema() { return localTableSchema; }
            public String getLocalTableName() { return localTableName; }
          }
      }
    
    class NullTable extends Table { }
    
    String stripTableName(String name)
      {
        return stripName(name, stripTablePrefixes, stripTableSuffixes);
      }
    
    String stripColumnName(String name)
      {
        return stripName(name, stripColumnPrefixes, stripColumnSuffixes);
      }
    
    static String stripName(String name, Set prefixes, Set suffixes)
      {
        String prefix, namePart;
        
        if (prefixes != null)
          for (Iterator it = prefixes.iterator(); it.hasNext();)
            {
              prefix = (String)it.next();
              
              if (name.length() > prefix.length())
                {
                  namePart = name.substring(0, prefix.length());

                  if (namePart.equalsIgnoreCase(prefix))
                    {
                      name = name.substring(prefix.length());
                      break;
                    }
                }
            }
        
        String suffix;
        
        if (suffixes != null)
          for (Iterator it = suffixes.iterator(); it.hasNext();)
            {
              suffix = (String)it.next();
              
              if (name.length() > suffix.length())
                {
                  namePart = name.substring(name.length() - suffix.length());

                  if (namePart.equalsIgnoreCase(suffix))
                    {
                      name = name.substring(0, name.length() - suffix.length());
                      break;
                    }
                }
            }
        
        return name;
      }
    
    static String normalizeName(String name)
      {
        return name.replaceAll("_","").toLowerCase();
      }
  }

