CREATE TABLE IF NOT EXISTS Production.Product
(
ProductID INT(3) NOT NULL,
Name VARCHAR(32),
ProductNumber VARCHAR(10),
MakeFlag INT(1),
FinishedGoodsFlag INT(1),
Color VARCHAR(12),
SafetyStockLevel INT(4),
ReorderPoint INT(3),
StandardCost FLOAT(9),
ListPrice FLOAT(7),
Size VARCHAR(3),
SizeUnitMeasureCode VARCHAR(3),
WeightUnitMeasureCode VARCHAR(3),
Weight FLOAT(6),
DaysToManufacture INT(1),
ProductLine VARCHAR(3),
Class VARCHAR(3),
Style VARCHAR(3),
ProductSubcategoryID INT(4),
ProductModelID INT(5),
SellStartDate DATE,
SellEndDate DATE,
DiscontinuedDate DATE,
rowguid VARCHAR(36),
ModifiedDate DATETIME,
PRIMARY KEY (ProductID)
)