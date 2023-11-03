para practica operaciones con rdd en spark

blog: https://chalimbu.blogspot.com/2023/11/rdd-en-spark-con-scala.html


dataset de prueba sacado de https://www.kaggle.com/datasets/isabbaggin/transaction-fraudulent-financial-syntheticdata/
transaction data for fraud analysis
la estructura del dataset es

- Transaction_Id
- Customer_Id
- Merchant_Id
- Amount
- Transaction time
- Is_fraudulent
- Card_type
- Location
- Purchase_category
- Customer_Age



quiero practicar respondiendo algunas preguntas.

- total de valores de transaciones por cada cliente
- valor promedio de las operaciones fraudulentas
- promedio valor de operaciones por cada cliente
- generar grupos de edades 0-20, 21 a 40, 41 a 60 etc, y contar el numero de operaciones 
fraudulentas por edad, ordenar por los grupos que tienen mas operaciones fraudulentas
