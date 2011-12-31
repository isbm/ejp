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

public class CustomerOrder 
  {
    private String customerId, firstName, lastName, email, product, status;
    private Integer quantity;
    private Double price;

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String id) { customerId = id; }
    public String getFirstName() { return firstName; }
    public void setFirstName(String fName) { firstName = fName; }
    public String getLastName() { return lastName; }
    public void setLastName(String lName) { lastName = lName; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String toString()
      {
        return customerId + ", " + firstName + ", " + lastName 
                + ", " + email + ", " + product + ", " + quantity 
                + ", " + price + ", " + status + "\n";
      }
  }
