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

import java.util.ArrayList;
import java.util.List;

/**
 * Customer represents the customers table and is associated with orders and support.
 */
public class Customer
  {
    private String customerId, password, firstName, lastName, companyName, email;
    private List<Support> support = new ArrayList<Support>();
    private List<Order> orders = new ArrayList<Order>();

    public Customer() {}

    public Customer(String customerId)
      {
        this.customerId = customerId;
      }

    public Customer(String customerId, String password, String firstName, 
                   String lastName, String companyName, String email)
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

    // Associations
    public List<Support> getSupport() { return support; }
    public void setSupport(List<Support> support) { this.support = support; }
    public List<Order> getOrders() { return orders; }
    public void setOrders(List<Order> orders) { this.orders = orders; }

    public String toString()
      {
        String returnString = customerId + ", " + firstName + ", " + lastName 
                            + ", " + companyName + ", " + email + "\n";

        if (support != null)
          for (Support s : support)
            returnString += s + "\n";

        if (orders != null)
          for (Order o : orders)
            returnString += o + "\n";

        return returnString;
      }
  }
