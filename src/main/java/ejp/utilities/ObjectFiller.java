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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * Object filler will fill an objects public members with values from a map.
 */

@SuppressWarnings("unchecked")
public class ObjectFiller
  {
    public static ItemNotFoundException itemNotFoundException = new ItemNotFoundException();

  /**
   * Fill the object with values from the given getHandler.
   * 
   * @param getHandler the GetHandler providing values for the object being filled
   * @param object the object being filled
   *
   * @return the object passed in
   */
  
    public static <T> T fillObject(GetHandler getHandler, T object) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        return fillObject(getHandler, object, null, true, false, null, false);
      }
    
  /**
   * Fill the object with values from the given getHandler.
   * 
   * @param getHandler the GetHandler providing values for the object being filled
   * @param object the object being filled
   * @param ignoreValueNames ignore the value names in the set
   * @param setNulls set or ignore null values
   *
   * @return the object passed in
   */
  
    public static <T> T fillObject(GetHandler getHandler, T object, Set<String> ignoreValueNames, boolean setNulls) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        return fillObject(getHandler, object, null, true, false, ignoreValueNames, setNulls);
      }
    
  /**
   * Fill the object with values from the given getHandler.
   * 
   * @param getHandler the GetHandler providing values for the object being filled
   * @param object the object being filled
   * @param fillPublicSetMethods fill public set methods of the object
   * @param fillPublicFields fill public fields of the object
   * @param ignoreValueNames ignore the value names in the set
   * @param setNulls set or ignore null values
   *
   * @return the object passed in
   */
  
    public static <T> T fillObject(GetHandler getHandler, T object, boolean fillPublicSetMethods, boolean fillPublicFields, Set<String> ignoreValueNames, boolean setNulls) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        return fillObject(getHandler, object, null, fillPublicSetMethods, fillPublicFields, ignoreValueNames, setNulls);
      }

  /**
   * Fill the object with values from the given getHandler.
   * 
   * @param getHandler the GetHandler providing values for the object being filled
   * @param object the object being filled
   * @param c the declaring class that methods and fields must exist in or null for no restriction
   * @param fillPublicSetMethods fill public set methods of the object
   * @param fillPublicFields fill public fields of the object
   * @param ignoreValueNames ignore the value names in the set
   * @param setNulls set or ignore null values
   *
   * @return the object passed in
   */
  
    public static <T> T fillObject(GetHandler getHandler, T object, Class<T> c, boolean fillPublicSetMethods, boolean fillPublicFields, Set<String> ignoreValueNames, boolean setNulls) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException
      {
        Set memberSet = new HashSet();
        
        if (fillPublicSetMethods)
          {
            Method[] methods = c != null ? c.getMethods() : object.getClass().getMethods();
            Method method = null;
            String methodName = null;
            String valueName = null;
            Object value = null;
            
            for (int i = 0; i < methods.length; i++)
              {
                method = methods[i];
                methodName = method.getName();

                if ((c == null || methods[i].getDeclaringClass().equals(c)) && methodName.startsWith("set") && !methodName.equalsIgnoreCase("set"))
                  {
                    valueName = methodName.substring(3,4).toLowerCase() + methodName.substring(4);

                    if (!memberSet.contains(valueName))
                      {
                        memberSet.add(valueName);

                        if (method.getParameterTypes().length > 0 && (ignoreValueNames == null || !ignoreValueNames.contains(valueName)))
                          {
                            try
                              {
                                value = getHandler.get(valueName, method.getParameterTypes()[0]);

                                if (value == null)
                                  value = getHandler.get(StringUtils.camelCaseToLowerCaseUnderline(valueName), method.getParameterTypes()[0]);

                                if (value == null)
                                  value = getHandler.get(valueName.toLowerCase(), method.getParameterTypes()[0]);

                                if ((setNulls || value != null) && method.getParameterTypes() != null && method.getParameterTypes().length == 1)
                                  method.invoke(object, new Object[] { ObjectConverter.convertObject(method.getParameterTypes()[0], value) });
                              }
                            catch (ItemNotFoundException e) { }
                          }
                      }
                  }
              }
          }
        
        if (fillPublicFields)
          {
            Field[] fields = c != null ? c.getFields() : object.getClass().getFields();
            String fieldName = null;
            Object value = null;
            
            for (int i = 0; i < fields.length; i++)
              {
                fieldName = fields[i].getName();
                
                if ((c == null || fields[i].getDeclaringClass().equals(c)) && !memberSet.contains(fieldName))
                  {
                    memberSet.add(fieldName);

                    if (ignoreValueNames == null || !ignoreValueNames.contains(fieldName))
                      {
                        try
                          {
                            value = getHandler.get(fieldName, fields[i].getType());

                            if (value == null)
                              value = getHandler.get(fieldName.toLowerCase(), fields[i].getType());

                            if (value == null)
                              value = getHandler.get(StringUtils.camelCaseToLowerCaseUnderline(fieldName), fields[i].getType());

                            if (setNulls || value != null)
                              fields[i].set(object, new Object[] { ObjectConverter.convertObject(fields[i].getType(), value) });
                          }
                        catch (ItemNotFoundException e) { }
                      }
                  }
              }
          }
        
        return object;
      }

    /**
     * Implement this interface for ObjectFiller to gain access to your data.
     * If your object has a field that doesn't match up to data, 
     * throw ItemNotFoundException instead of returning null so ObjectFiller 
     * doesn't set the field to null.
     */
    public interface GetHandler
      {
        public <T> T get(String key, Class<T> objectType) throws ItemNotFoundException;
      }
    
    public static class ItemNotFoundException extends Exception
      {
        private static final long serialVersionUID = 100L;
      }
  }

