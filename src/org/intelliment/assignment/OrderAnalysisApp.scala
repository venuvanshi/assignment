/*
  Author - Venuvanshi Bhujbal
  Purpose - Intelliment Assignments
  Title - OrderDetailAnalysis 
*/

package org.intelliment.assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object OrderAnalysisApp {

  def main(args: Array[String]) {
    
    System.setProperty("hadoop.home.dir", "C:/winutils");

    val sparksession = SparkSession.builder
      .appName(" Sales Order Analysis App")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///c:tmp/spark-warehouse")
      .getOrCreate

    import sparksession.implicits._

    val orderFilePath = "S:/Venuvanshi_Storage/Intelliment/assignment/assignment/Input_dataSet/Orders.txt"
    val customerFilePath = "S:/Venuvanshi_Storage/Intelliment/assignment/assignment/Input_dataSet/Customers.txt"
    val productFilePath = "S:/Venuvanshi_Storage/Intelliment/assignment/assignment/Input_dataSet/Products.txt"
    val orderItemFilePath = "S:/Venuvanshi_Storage/Intelliment/assignment/assignment/Input_dataSet/Order_Items.txt"

    val order = sparksession.read
      .option("header", "false")
      .option("inferschema", "false")
      .csv(orderFilePath)

    val customer = sparksession.read
      .option("header", "false")
      .option("inferschema", "false")
      .option("delimiter", ";")
      .csv(customerFilePath)

    val orderitems = sparksession.read
      .option("header", "false")
      .option("inferschema", "false")
      .csv(orderItemFilePath)

    val products = sparksession.read
      .option("header", "false")
      .option("inferschema", "false")
      .option("delimiter", ";")
      .csv(productFilePath)

//    order.show()
//    customer.show()
//    orderitems.show()
//    products.show()

    val orderdf = order.select(
      order("_c0").cast(IntegerType).as("order_id"),
      order("_c1").cast(DateType).as("order_date"),
      order("_c2").cast(IntegerType).as("order_customer"),
      order("_c3").cast(StringType).as("order_status"))

    val customerdf = customer.select(
      customer("_c0").cast(IntegerType).as("customer_id"),
      customer("_c1").cast(StringType).as("customer_fname"),
      customer("_c2").cast(StringType).as("customer_lname"),
      customer("_c3").cast(StringType).as("customer_email"),
      customer("_c4").cast(StringType).as("customer_password"),
      customer("_c5").cast(StringType).as("customer_street"),
      customer("_c6").cast(StringType).as("customer_city"),
      customer("_c7").cast(StringType).as("customer_state"))

    val orderitemsdf = orderitems.select(
      orderitems("_c0").cast(IntegerType).as("order_item_id"),
      orderitems("_c1").cast(IntegerType).as("order_item_order_id"),
      orderitems("_c2").cast(IntegerType).as("order_item_product_id"),
      orderitems("_c3").cast(IntegerType).as("order_item_quantity"),
      orderitems("_c4").cast(FloatType).as("order_item_subtotal"),
      orderitems("_c5").cast(FloatType).as("order_item_product_price"))

    val productsdf = products.select(
      products("_c0").cast(IntegerType).as("product_id"),
      products("_c1").cast(IntegerType).as("product_category_id"),
      products("_c2").cast(StringType).as("product_name"),
      products("_c3").cast(StringType).as("product_description"),
      products("_c4").cast(FloatType).as("product_price"),
      products("_c5").cast(StringType).as("product_float"))

//    orderdf.show()
//    customerdf.show()
//    orderitemsdf.show()
//    productsdf.show()
    
    
//1. Total sales for each date (hint: join orders & order_items)
    val joinedOrder = orderdf.as('a).join(orderitemsdf.as('b), $"a.order_id" === $"b.order_item_order_id")
    //joinedOrder.show()
    joinedOrder.createOrReplaceTempView("orderdetail")
    val totalSalesDate = sparksession.sql("SELECT order_date, SUM(order_item_product_price) FROM orderdetail  GROUP BY order_date ORDER BY order_date ").show()
    
//2. Total sales for each month     
    val totalSalesMonth = sparksession.sql("SELECT month(order_date), SUM(order_item_product_price) FROM orderdetail  GROUP BY month(order_date) ORDER BY month(order_date) ").show()
    
//3. Average sales for each date
    val avgSalesDate = sparksession.sql("SELECT order_date, Avg(order_item_product_price) FROM orderdetail  GROUP BY order_date ORDER BY order_date ").show()
    
    
//4. Average sales for each month 
    val avgSalesMonth = sparksession.sql("SELECT month(order_date), Avg(order_item_product_price) FROM orderdetail  GROUP BY month(order_date) ORDER BY month(order_date) ").show()
    
//5. Name of Month having highest sale    
    val topSalesMonth = sparksession.sql("SELECT month(order_date), SUM(order_item_product_price) FROM orderdetail  GROUP BY month(order_date) ORDER BY 2 DESC LIMIT 1  ").show()
   
//6. Top 10 revenue generating products
    productsdf.createOrReplaceTempView("productsdf")
    orderitemsdf.createOrReplaceTempView("orderitemsdf")
    val top10products = sparksession.sql("Select product_name from productsdf inner join orderitemsdf on product_id =order_item_product_id group by Product_name order by sum(order_item_product_price) desc limit 10").show()

                       
//7.* Top 3 purchased customers for each day/month
    val customerorder =  joinedOrder.join(customerdf,customerdf("customer_id") === joinedOrder("order_customer"))
                       .createOrReplaceTempView("customerorder")
   customerdf.createOrReplaceTempView("customerdf")

   //7a. Top 3 purchased customers for each day
   val top3customerperday =sparksession.sql("Select concat(customer_fname,' ',customer_lname), order_date, customersales from (Select  customer_id,order_date,sum(order_item_product_price) as customersales, Row_number() over (partition by order_date order by sum(order_item_product_price) desc) as rn from customerorder Group by order_date,customer_id ) temp inner join customerdf on customerdf.customer_id=temp.customer_id where rn<=3 order by order_date,customersales desc").show()
       
       
   //7b. Top 3 purchased customers for each month
   val top3customerpermonth =sparksession.sql("Select concat(customer_fname,' ',customer_lname) as customerName, month_number, customersales from (Select  customer_id,month(order_date) as month_number,sum(order_item_product_price) as customersales, Row_number() over (partition by month(order_date) order by sum(order_item_product_price) desc) as rn from customerorder Group by month(order_date),customer_id ) temp inner join customerdf on customerdf.customer_id=temp.customer_id where rn<=3 order by month_number,customersales desc").show()
   
    
//8. Most sold products for each day/month
  
   val productorder = joinedOrder.join(productsdf,productsdf("product_id") === joinedOrder("order_item_product_id"))
                       .createOrReplaceTempView("productorder")

   productsdf.createOrReplaceTempView("productsdf")
   
       
   //8a. Most sold products for each day
   val topproductperday =sparksession.sql("Select product_name, order_date, productsales from (Select  product_id,order_date,sum(order_item_product_price) as productsales, Row_number() over (partition by order_date order by sum(order_item_product_price) desc) as rn from productorder Group by order_date,product_id ) temp inner join productsdf on productsdf.product_id=temp.product_id where rn=1 order by order_date,productsales desc").show()
       
   //8b. Most sold products for each month
   val topproductpermonth =sparksession.sql("Select product_name, month_number, productsales from (Select  product_id,month(order_date) as month_number,sum(order_item_product_price) as productsales, Row_number() over (partition by month(order_date) order by sum(order_item_product_price) desc) as rn from productorder Group by month(order_date),product_id ) temp inner join productsdf on productsdf.product_id=temp.product_id where rn=1 order by month_number,productsales desc").show()
   
//9. Count of distinct Customer, group by State  (use customer table)
   
   val customercountByState = sparksession.sql("SELECT  customer_state, COUNT(DISTINCT customer_id) as Customer_Count FROM customerdf GROUP BY customer_state ORDER BY customer_state").show() 

//10. Most popular product category
        
  val mostpopularproduct = sparksession.sql("Select product_name as mostpopularproduct,totalordersplaced from (Select product_id,sum(order_item_quantity) as totalordersplaced from productorder Group by product_id order by sum(order_item_quantity) desc limit 1) temp inner join productsdf on temp.product_id = productsdf.product_id").show()


  }

}
