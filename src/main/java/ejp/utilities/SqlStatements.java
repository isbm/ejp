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

import ejp.Database;
import ejp.DatabaseException;
import ejp.DatabaseManager;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A simple SQL manager for handling soft coded SQL statements.
 * @deprecated use ejp.SqlStatements instead
 */
@Deprecated
public class SqlStatements
  {
    private Map sqlStatements;

    /**
     * Loads SQL statements based on a supplied query.  The SQL statements are
     * loaded into a map based on column 1 (id/key column) and column 2
     * (SQL statement/value column) of your SQL query.
     *
     * @param sql the SQL statement that loads the statements (e.g. "select sql_id, sql_statement from sql_statements")
     *
     * @throws DatabaseException
     * @deprecated use ejp.SqlStatements instead
     */
    @Deprecated
    public void loadSQLStatements(DatabaseManager dbm, String sql) throws DatabaseException
      {
        Database db = dbm.getDatabase();

        try
          {
            sqlStatements = ResultSetUtils.loadMap(db.executeQuery(sql).getResultSet(), new HashMap());
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
     * Loads SQL statements from a Java style properties file in the format of:
     *
     * sqlStatementName = SQL statement
     *
     * @param file the properties file
     *
     * @throws DatabaseException
     * @deprecated use ejp.SqlStatements instead
     */
    @Deprecated
    public void loadSQLStatements(File file) throws FileNotFoundException, IOException
      {
        Properties props = new Properties();
        FileReader reader = new FileReader(file);

        try
          {
            props.load(reader);

            sqlStatements = props;
          }
        finally
          {
            reader.close();
          }
      }

    /**
     * Set the map that sql statements are retrieved from (used in place of loadSqlStatements()).
     *
     * @param sqlStatements the SQL statements map that sql statements are retrieved from
     *
     * @throws DatabaseException
     * @deprecated use ejp.SqlStatements instead
     */
    @Deprecated
    public void setSQLStatements(Map sqlStatements) throws DatabaseException
      {
        this.sqlStatements = sqlStatements;
      }

    /**
     * Returns the SQL statement associated with the SQL Id (key column from loadSqlStatements()).
     *
     * @param key the sql id
     *
     * @return the SQL statement associated with the key (SQL Id)
     *
     * @throws DatabaseException
     * @deprecated use ejp.SqlStatements instead
     */
    @Deprecated
    public String getSQLStatement(String key) throws DatabaseException
      {
        if (sqlStatements == null)
          throw new DatabaseException("SQL Statements have not been initialized");

        String sql = (String)sqlStatements.get(key);
        
        if (sql == null)
          throw new DatabaseException("SQL Statement is null");

        return sql;
      }
  }
