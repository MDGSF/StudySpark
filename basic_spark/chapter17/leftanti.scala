import spark.implicits._
import org.apache.spark.sql.DataFrame
 
// 创建员工信息表
val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
val employees: DataFrame = seq.toDF("id", "name", "age", "gender")
 
// 创建薪资表
val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
val salaries:DataFrame = seq2.toDF("id", "salary")

val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "leftanti") 

salaries.show
employees.show
jointDF.show


/*
scala> salaries.show
employees.show
jointDF.show
+---+------+
| id|salary|
+---+------+
|  1| 26000|
|  2| 30000|
|  4| 25000|
|  3| 20000|
+---+------+


scala> +---+-------+---+------+
| id|   name|age|gender|
+---+-------+---+------+
|  1|   Mike| 28|  Male|
|  2|   Lily| 30|Female|
|  3|Raymond| 26|  Male|
|  5|   Dave| 36|  Male|
+---+-------+---+------+


scala> +---+------+
| id|salary|
+---+------+
|  4| 25000|
+---+------+
*/
