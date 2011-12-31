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
 * Order represents the orders table and is associated with customers
 */
public class Order // extends PersistentObject // optional for caching
  {
    // keys are all handled behind the scenes; map them if needed
    //private Long orderId;
    private Integer quantity;
    private Double price;
    private String product, status; // customerId, 

    public Order() {}

    public Order(String product, Integer quantity, Double price, String status)
      {
        this.product = product;
        this.quantity = quantity;
        this.price = price;
        this.status = status;
      }

    // keys are all handled behind the scenes; map them if needed

    //public Long getOrderId() { return orderId; }
    //public void setOrderId(Long orderId) { this.orderId = orderId; }
    //public String getCustomerId() { return customerId;}
    //public void setCustomerId(String customerId) { this.customerId = customerId; }
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
        // orderId + ", " + customerId + ", "
        return quantity + ", " + price + ", " + product + ", " + status; 
      }
  }
