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

import ejp.PersistenceManager.ObjectInformation;
import java.io.Serializable;

/**
 * <p>Because EJP keeps track of your objects persistence and keys, and does so 
 * external of your objects, you must cache objects through EJP.   
 *
 * <p>However, if your cache does not off-load objects to external storage and 
 * the objects are always in memory, then caching does not have to be handled 
 * through EJP.  The flip-side is If your cache uses external storage, you 
 * must handle caching through EJP.
 *
 * <p>To implement caching with EJP, simply implement CacheInterface:
 * <pre>
 *      public interface CacheInterface 
 *        {
 *          public Object get(Object key);
 *          public void put(Object key, Object obj);
 *        }
 * </pre>
 * 
 * <p>The get and put methods simply perform what ever is needed of your caching 
 * manager (Ehcache, Memcached, etc).  You then register your implementation 
 * of CacheInterface by creating an instance of DatabaseCache.  From that 
 * point you need to use the methods DatabaseCache.put() and 
 * DatabaseCache.get() for all your object caching.
 *
 * @author db
 */
@SuppressWarnings("unchecked")
public class DatabaseCache 
  {
    CacheInterface cache;
    
    /**
     * Register your EJP database object caching implementation.
     * @param cache your caching implementation
     */
    public DatabaseCache(CacheInterface cache)
      {
        this.cache = cache;
      }
    
    /**
     * You must use this method to add objects to your registered cache.
     * 
     * @param key the search key for caching
     * @param value the value you wish to cache
     * @throws DatabaseException 
     */
    public <T> void put(Object key, T value) throws DatabaseException
      {
        cache.put(key, new CacheItem(value, PersistenceManager.get(value)));
      }

    /**
     * You must use this method to get objects from your registered cache.
     * 
     * @param key the search key for caching
     * @return the value previously stored in the cache with cachePut.
     * @throws DatabaseException 
     */
    public <T> T get(Object key) throws DatabaseException
      {
        CacheItem ci = (CacheItem)cache.get(key);

        if (ci != null)
          {
            if (!PersistenceManager.isPersistent(ci.value) && ci.info != null)
              {
                ObjectInformation info = PersistenceManager.get(ci.value);
                info.isPersistent = ci.info.isPersistent;
                info.keyValues = ci.info.keyValues;
              }

            return (T)ci.value;
          }
        
        return null;
      }
    
    static class CacheItem implements Serializable
      {
        public Object value;
        ObjectInformation info;
        
        CacheItem(Object value, ObjectInformation info)
          {
            this.value = value;
            this.info = info;
          }
      }

    /**
     * <p>Interface for implementing a cache for EJP database objects.  This is 
     * required for caching EJP database objects due to the fact that EJP 
     * associates information with your persistent objects.</p>
     * 
     * <p>If your cache is in memory and does not remove objects from memory to  
     * external storage, then you do not need to worry about implementing this interface.  
     * If your cache does use external storage, then you need to handle caching through 
     * EJP via this interface so EJP can manage persistence for your cached 
     * objects when loaded from external storage.</p>
     * 
     * <p>The good news is all you have to do is implement this interface's get and 
     * put methods, then register it with your EJP DatabaseCache, and use 
     * DatabaseCache.put and DatabaseCache.get to store and retrieve 
     * your objects.</p>
     */
    public interface CacheInterface
      {
        /**
         * Perform anything required to retrieve objects from your cache.
         * @param key the key to search on
         */
        Object get(Object key);

        /**
         * Perform anything required to store objects in your cache.
         * @param key the key to search on
         * @param object object to store in your cache
         */
        void put(Object key, Object object);
      }
  }
