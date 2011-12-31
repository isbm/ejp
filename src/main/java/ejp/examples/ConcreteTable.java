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
import ejp.annotations.ConcreteTableInheritance;
import java.util.ArrayList;
import java.util.Collection;

public class ConcreteTable 
  {
    public static void main(String[] args) throws DatabaseException
      {
        DatabaseManager dbm = DatabaseManager.getDatabaseManager("ejp");

        dbm.executeUpdate("delete from customer_support");

        dbm.saveObject(new CustomerSupport("alincoln", "mypasswd1", "Abraham", "Lincoln", "United States", "alincoln@unitedstates.gov",
                                             "Request", "New", "no phone", "Can I have my bust on a penny, please."));

        dbm.saveObject(new CustomerSupport("tjefferson", "mypasswd2", "Thomas", "Jefferson", "United States", "tjefferson@unitedstates.gov",
                                             "Request", "New", "no phone", "Can I have my bust on a nickel, please."));

        Collection<CustomerSupport> customerSupport = dbm.loadObjects(new ArrayList<CustomerSupport>(), CustomerSupport.class);

        for (CustomerSupport c : customerSupport)
          System.out.println(c);
      }
  
    public static class Customer
      {
        String customerId, password, firstName, lastName, companyName, email;

        public Customer() {}

        public Customer(String customerId, String password, String firstName, String lastName, String companyName, String email)
          {
            this.customerId = customerId;
            this.password = password;
            this.firstName = firstName;
            this.lastName = lastName;
            this.companyName = companyName;
            this.email = email;
          }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String id) { customerId = id; }
        public String getPassword() { return password; }
        public void setPassword(String passwd) { password = passwd; }
        public String getFirstName() { return firstName; }
        public void setFirstName(String fName) { firstName = fName; }
        public String getLastName() { return lastName; }
        public void setLastName(String lName) { lastName = lName; }
        public String getCompanyName() { return companyName; }
        public void setCompanyName(String name) { companyName = name; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
      }
    
    @ConcreteTableInheritance
    public static class CustomerSupport extends Customer
      {
        private String code, status, request, phone;

        public CustomerSupport() {}
        
        public CustomerSupport(String customerId, String password, String firstName, String lastName, String companyName,
                                  String email, String code, String status, String phone, String request)
          {
            super(customerId, password, firstName, lastName, companyName, email);
            
            this.code = code;
            this.status = status;
            this.phone = phone;
            this.request = request;
          }
        
        public String getCode() { return code; }
        public void setCode(String code) { this.code = code; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getRequest() { return request; }
        public void setRequest(String request) { this.request = request; }
        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
        
        public String toString()
          {
            return customerId + ", " + password + ", " + firstName + ", " + lastName + ", " + companyName
                   + ", " + email + ", " + code + ", " + status + ", " + phone + ", " + request;
          }
      }
  }
