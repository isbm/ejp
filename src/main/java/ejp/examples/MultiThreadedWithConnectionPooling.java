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

package ejp.examples;

import ejp.DatabaseException;
import ejp.DatabaseManager;
import ejp.UpdateManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

/**
 * The following inserts 1 million dogs (in ~12 seconds) into the dogs table.  
 * 
 * Example submitted by cqlang.
 */

@SuppressWarnings("unchecked")
public class MultiThreadedWithConnectionPooling 
  {
    static DataSource getDbcpDataSource()
      {
        BasicDataSource ds = new BasicDataSource();
        
        ds.setDriverClassName("org.hsqldb.jdbcDriver");
        ds.setUsername("sa");
        ds.setPassword("");
        ds.setMaxActive(100);
        ds.setUrl("jdbc:hsqldb:mem:ejp_example");
        
        return ds;
      }
    
    public static void main(String[] args) throws DatabaseException, InterruptedException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp", 100, getDbcpDataSource());
        
        dbm.executeUpdate("CREATE TABLE IF NOT EXISTS DOG (NAME VARCHAR(40), AGE INT NOT NULL)");
        
        execute(dbm);
      }
    
    static void execute(final DatabaseManager dbm) throws DatabaseException, InterruptedException
      {
        long time = System.currentTimeMillis();
        ExecutorService exec = Executors.newFixedThreadPool(100);
        
        System.out.println("\n\nWorking ...");
        
        Runnable runnable = new Runnable() 
          {
            public void run()
              {
                for (int t = 0; t < 100; t++) 
                  {
                    try 
                      {
                        new UpdateManager(dbm) 
                          {
                            public void run() throws DatabaseException
                              {
                                for (int j = 0; j < 100; j++) 
                                  {
                                    saveObject(new Dog(String.valueOf(Count.get()), Count.get()));
                                    Count.count();
                                  }
                              }
                          }.executeBatchUpdates();
                      } catch (DatabaseException e) { e.printStackTrace(); }
                  }
              }
          };

        for (int i = 0; i < 100; i++) 
          {
            exec.execute(runnable);
          }

        exec.shutdown();
        exec.awaitTermination(100, TimeUnit.SECONDS);
        time = (System.currentTimeMillis() - time) / 1000;

        System.out.println("\n\n" + Count.count + " dogs added to database in " + time + " seconds");
        
        Long count = ((Collection<Long>)dbm.executeQuery(new ArrayList<Long>(), true, "select count(*) from dog")).toArray(new Long[1])[0];
        System.out.println("select count(*) from dog = " + count);
      }

    public static class Count 
      {
        static int count = 0;

        public synchronized static void count() { Count.count++; }
        public static int get() { return Count.count; }
      }

    public static class Dog 
      {
        private int age;
        private String name;

        public Dog() { super(); }
        public Dog(String name, int age) { super(); this.name = name; this.age = age; }
        public int getAge() { return this.age; }
        public String getName() { return this.name; }
        public void setAge(int age) { this.age = age; }
        public void setName(String name) { this.name = name; }
      }
  }
