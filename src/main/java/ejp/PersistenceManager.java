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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * <p>Persistence manager automatically tracks persistence for any object used in 
 * loads, saves, and deletes.</p>
 * 
 * <p>It does so by storing information associated with your objects in 
 * ObjectInformation and then storing the object information in a WeakHashMap
 * keyed with your objects.</p>
 * 
 * <p>You can override this functionality by implementing PersistentObjectInterface 
 * or extending PersistentObject.</p>
 */

final public class PersistenceManager
  {
    private static Map<Object, ObjectInformation> objectMap = Collections.synchronizedMap(new WeakHashMap<Object, ObjectInformation>(4096));

    /**
     * Clear the persistence cache.
     */
    public static void clearPersistenceCache() { objectMap.clear(); }

    /**
     * Remove/detach an object from the persistence manager.  Makes the 
     * object transient, but does not delete the object from the database.
     *
     * @param object The object to remove
     */
    public static void remove(Object object)
      {
        objectMap.remove(object);
      }

    /**
     * Check the persistent state of the object.  True if the object persists (has been saved/loaded) and exists in the database.
     *
     * @param object The object to check
     *
     * @return true if the object persists
     */
    public static boolean isPersistent(Object object)
      {
        return get(object).isPersistent;
      }

    static Map<Object, ObjectInformation> getObjectMap() { return objectMap; }

    static ObjectInformation get(Object object)
      {
        if (object instanceof PersistentObjectInterface)
          return ((PersistentObjectInterface)object).getObjectInformation();

        ObjectInformation p = objectMap.get(object);

        if (p == null)
          objectMap.put(object, p = new ObjectInformation());
        
        return p;
      }

    /**
     * This class contains the information that EJP uses to 
     * track the persistent state of your objects.  Your only need
     * for this class is with regard to implementing PersistentObjectInterface.
     */
    public static class ObjectInformation implements Serializable
      {
        boolean isPersistent;
        Map<String, Object> keyValues = new HashMap<String, Object>();
      }
    
    /**
     * <p>This interface is implemented to provide persistence tracking information 
     * for your objects.  It is only needed when caching objects, and those 
     * objects are being ejected from and returned to main memory.  This is an 
     * alternative to implementing DatabaseCache.CacheInterface.</p>
     * 
     * <p>Implementing this interface also overrides the default behavior of 
     * storing EJP object information in a WeakHashMap.</p>
     * 
     * <p>All that is required is to have the following defined in your class:</p>
     * <pre>
     *       ObjectInformation objectInformation = new ObjectInformation();
     * </pre>
     * <p>and to return it with the single method (getObjectInformation())
     * of this interface.</p>
     */
    public interface PersistentObjectInterface extends Serializable
      {
        public ObjectInformation getObjectInformation();
      }

    /**
     * You can extend this class in place of implementing PersistentObjectInterface.
     */
    public static class PersistentObject implements PersistentObjectInterface
      {
        ObjectInformation objectInformation = new ObjectInformation();
        
        public ObjectInformation getObjectInformation() { return objectInformation; }
      }
  }
