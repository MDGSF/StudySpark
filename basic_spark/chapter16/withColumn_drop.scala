import org.apache.spark.sql.DataFrame
import spark.implicits._
 
val employees = Seq((1, "John", 26, "Male"), (2, "Lily", 28, "Female"), (3, "Raymond", 30, "Male"))
val employeesDF: DataFrame = employees.toDF("id", "name", "age", "gender")
 
employeesDF.withColumn("crypto", hash($"age")).drop("age").show

/*
+---+-------+---+------+-----------+
| id|   name|age|gender|     crypto|
+---+-------+---+------+-----------+
|  1|   John| 26|  Male|-1223696181|
|  2|   Lily| 28|Female|-1721654386|
|  3|Raymond| 30|  Male| 1796998381|
+---+-------+---+------+-----------+
*/