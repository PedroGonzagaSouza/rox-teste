import pyspark.sql.functions as f
import pyspark as spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

spark = SparkSession.builder.getOrCreate()

person_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Person.Person.csv")
product_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Production.Product.csv")
sale_costumer_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Sales.Customer.csv")
sales_orderDetail_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Sales.SalesOrderDetail.csv")
sales_orderHeader_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Sales.SalesOrderHeader.csv")
sales_specialOffer_df = spark.read.options(delimiter=';', header=True).csv("csv_files/Sales.SpecialOfferProduct.csv")


person_df_tratado = person_df\
.withColumn('BusinessEntityID', f.col('BusinessEntityID').cast('int'))\
.withColumn('NameStyle', f.col('NameStyle').cast('int'))\
.withColumn('EmailPromotion', f.col('EmailPromotion').cast('int'))\
.withColumn('ModifiedDate', f.col('ModifiedDate').cast('date'))

person_df_tratado = person_df_tratado.replace({'NULL':None})

""" =================================================== """

product_df_tratado = product_df\
.withColumn('ProductID', f.col('ProductID').cast('int'))\
.withColumn('MakeFlag', f.col('MakeFlag').cast('boolean'))\
.withColumn('FinishedGoodsFlag', f.col('FinishedGoodsFlag').cast('boolean'))\
.withColumn('SafetyStockLevel', f.col('SafetyStockLevel').cast('int'))\
.withColumn('ReorderPoint', f.col('ReorderPoint').cast('int'))\
.withColumn('StandardCost', f.col('StandardCost').cast('float'))\
.withColumn('ListPrice', f.col('ListPrice').cast('float'))\
.withColumn('Weight', f.col('Weight').cast('float'))\
.withColumn('DaysToManufacture', f.col('DaysToManufacture').cast('int'))\
.withColumn('ProductSubcategoryID', f.col('ProductSubcategoryID').cast('int'))\
.withColumn('ProductModelID', f.col('ProductModelID').cast('int'))\
.withColumn('SellStartDate', f.col('SellStartDate').cast('date'))\
.withColumn('SellEndDate', f.col('SellEndDate').cast('date'))\
.withColumn('DiscontinuedDate', f.col('DiscontinuedDate').cast('date'))\
.withColumn('ModifiedDate', f.col('ModifiedDate').cast('date'))

product_df_tratado = product_df_tratado.replace({'NULL':None})

""" =================================================== """

sale_costumer_df_tratado = sale_costumer_df\
.withColumn('CustomerID', f.col('CustomerID').cast('int'))\
.withColumn('PersonID', f.col('PersonID').cast('int'))\
.withColumn('StoreID', f.col('StoreID').cast('int'))\
.withColumn('TerritoryID', f.col('TerritoryID').cast('int'))\
.withColumn('ModifiedDate', f.col('ModifiedDate').cast('date'))
sale_costumer_df_tratado = sale_costumer_df_tratado.replace({'NULL':None})

""" ==================================================="""

sales_orderDetail_df_tratado = sales_orderDetail_df\
.withColumn('SalesOrderID', f.col('SalesOrderID').cast('int'))\
.withColumn('SalesOrderDetailID', f.col('SalesOrderDetailID').cast('int'))\
.withColumn('OrderQty', f.col('OrderQty').cast('int'))\
.withColumn('ProductID', f.col('ProductID').cast('int'))\
.withColumn('SpecialOfferID', f.col('SpecialOfferID').cast('int'))\
.withColumn('UnitPrice', f.col('UnitPrice').cast('float'))\
.withColumn('UnitPriceDiscount', f.col('UnitPriceDiscount').cast('float'))\
.withColumn('UnitPrice', f.col('UnitPrice').cast('float'))\
.withColumn('LineTotal', f.col('LineTotal').cast('float'))

sales_orderDetail_df_tratado = sales_orderDetail_df_tratado.replace({'NULL':None})

""" =================================================== """

sales_orderHeader_df_tratado = sales_orderHeader_df\
.withColumn("SubTotal", regexp_replace("SubTotal", ",", "."))\
.withColumn("TaxAmt", regexp_replace("TaxAmt", ",", "."))\
.withColumn("Freight", regexp_replace("Freight", ",", "."))\
.withColumn("TotalDue", regexp_replace("TotalDue", ",", "."))\
\
.withColumn('SalesOrderID', f.col('SalesOrderID').cast('int'))\
.withColumn('RevisionNumber', f.col('RevisionNumber').cast('int'))\
.withColumn('OrderDate', f.col('OrderDate').cast('date'))\
.withColumn('DueDate', f.col('DueDate').cast('date'))\
.withColumn('ShipDate', f.col('ShipDate').cast('date'))\
.withColumn('Status', f.col('Status').cast('int'))\
.withColumn('OnlineOrderFlag', f.col('OnlineOrderFlag').cast('int'))\
.withColumn('SalesOrderNumber', f.col('SalesOrderNumber').cast('int'))\
.withColumn('CustomerID', f.col('CustomerID').cast('int'))\
.withColumn('SalesPersonID', f.col('SalesPersonID').cast('int'))\
.withColumn('TerritoryID', f.col('TerritoryID').cast('int'))\
.withColumn('BillToAddressID', f.col('BillToAddressID').cast('int'))\
.withColumn('ShipToAddressID', f.col('ShipToAddressID').cast('int'))\
.withColumn('ShipMethodID', f.col('ShipMethodID').cast('int'))\
.withColumn('CreditCardID', f.col('CreditCardID').cast('int'))\
.withColumn('CurrencyRateID', f.col('CurrencyRateID').cast('int'))\
.withColumn('SubTotal', f.col('SubTotal').cast('float'))\
.withColumn('TaxAmt', f.col('TaxAmt').cast('float'))\
.withColumn('Freight', f.col('Freight').cast('float'))\
.withColumn('ModifiedDate', f.col('ModifiedDate').cast('date'))\
.withColumn('TotalDue', f.col('TotalDue').cast('float'))

sales_orderHeader_df_tratado = sales_orderHeader_df_tratado.replace({'NULL':None})

""" =================================================== """

sales_specialOffer_df_tratado = sales_specialOffer_df\
.withColumn('SpecialOfferID', f.col('SpecialOfferID').cast('int'))\
.withColumn('ProductID', f.col('ProductID').cast('int'))\
.withColumn('ModifiedDate', f.col('ModifiedDate').cast('date'))

sales_specialOffer_df_tratado = sales_specialOffer_df_tratado.replace({'NULL':None})



