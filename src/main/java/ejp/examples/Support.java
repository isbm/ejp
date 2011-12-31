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

/**
 * Support represents the support table and is associated with customers.
 */
public class Support // implements PersistentObjectInterface // optional for caching
  {
    // keys are all handled behind the scenes; map them if needed
    //private Long supportId;
    private String code, status, phone, email, request; // customerId, 

    public Support() {}

    public Support(String code, String status, String phone, String email, String request)
      {
        this.code = code;
        this.status = status;
        this.phone = phone;
        this.email = email;
        this.request = request;
      }

    // keys are all handled behind the scenes; map them if needed

    //public Long getSupportId() { return supportId; }
    //public void setSupportId(Long id) { supportId = id; }
    //public String getCustomerId() { return customerId; }
    //public void setCustomerId(String id) { customerId = id; }
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getRequest() { return request; }
    public void setRequest(String request) { this.request = request; }

    public String toString() 
      { 
        // supportId + ", " + customerId + ", " + 
        return code + "," + status + ", " + phone + ", " + email + ", " + request; 
      }

    /* optional
    ObjectInformation objectInformation = new ObjectInformation();

    public ObjectInformation getObjectInformation() { return objectInformation; }
     */
  }
