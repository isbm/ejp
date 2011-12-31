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

import ejp.DatabaseCache;
import ejp.DatabaseManager;
import ejp.DatabaseException;
import java.io.Serializable;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/*
 *  Because EJP keeps track of your objects persistence and keys, and does so external of
 *  your objects, you must cache objects through EJP. However, if your cache does not
 *  off-load objects to external storage and the objects are always in memory, then caching
 *  does not have to be handled through EJP. The flip-side is If your cache uses external
 *  storage, you must handle caching through EJP.
 * 
 *  To implement caching with EJP, simply implement CacheInterface:
 * 
 *  public interface CacheInterface
 *    {
 *      public Object get(Object key);
 *      public void put(Object key, Object obj);
 *    }
 * 
 *  The get and put methods simply perform what ever is needed of your caching manager
 *  (Ehcache, Memcached, etc). You then register your implementation of CacheInterface
 *  by creating an instance of DatabaseCache. From that point you need to use the
 *  methods DatabaseCache.put() and DatabaseCache.get() for all your object caching.
 * 
 *  An alternative to implementing CacheInterface is to implement 
 *  PersistenceManager.PersistentObjectInterface.  See Order.java and Support.java 
 *  for examples.
 * 
 *  All that is required is to have the following defined in your class:
 * 
 *        ObjectInformation objectInformation = new ObjectInformation();
 * 
 *  and to return it with the single method (getObjectInformation()) of this interface. This
 *  way the information EJP needs is stored in your object, and when your objects are
 *  cached, EJPs information will be cached as well.
 * 
 *  You can also simply extend PersistenceManager.PersistentObject, and nothing else is
 *  needed.
 */
public class Caching
  {
    public static void main(String[] args) throws DatabaseException, ClassNotFoundException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp_hsql");
        CreateDatabase.createHSQLDatabase(dbm);
        DatabaseCache cache = new DatabaseCache(new CacheExample());

        Customer customer;

        for (int i = 0; i < 1000000; i++)
          {
            customer = new Customer("username " + i, "password " + i, "first name " + i, "last name " + i, "company name " + i, "email " + i);
            dbm.saveObject(customer);
            cache.put("username " + i, customer);
          }

        for (int i = 0; i < 10; i+=2)
          {
            customer = cache.get("username " + i);

            if (customer != null)
              System.out.println(customer.getCustomerId() + ", " + customer.getFirstName() + ", " + customer.getLastName());
            else
              System.out.println("customer is null");
          }
      }
    
    /**
     * Simple implementation using Cache.
     */
    public static class CacheExample implements DatabaseCache.CacheInterface
      {
        Cache cache;

        /**
         * Creates a simple cache
         */
        CacheExample()
          {
            CacheManager cm = CacheManager.getInstance();
            cm.addCache(new Cache("test", 1000, true, true, 100, 100)); 
            cache = cm.getCache("test");
          }

        /**
         * Creates a cache that is defined in Cache's default XML file
         * @param cacheName the cache defined in the XML file
         */
        CacheExample(String cacheName) { cache = CacheManager.getInstance().getCache("test"); }

        public Object get(Object key)
          {
            Element element = cache.get((Serializable)key);
            
            if (element != null)
              return element.getValue();
            
            return null;
          }
        
        public void put(Object key, Object obj)
          {
            cache.put(new Element((Serializable)key, (Serializable)obj));
          }
      }
  }
