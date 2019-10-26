// Carregando a lib
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import sys.process._
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions._

//val spark = new SparkSession.builder.getOrCreate()
val sqlContext = spark.sqlContext

//val sc = new SparkContext()
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//"hdfs dfs -put dataset/dataset2.csv /user/spark/lab02/dataset/" !

// Lendo o arquivo CSV
var df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load("lab02/dataset/dataset2.csv")

// Visualizando o dataset carregado
df.printSchema()

df.show()

df.groupBy("Produto").agg(sum("Quantidade")).show()

//# Quantidade de produtos vendidos
df.select("Produto").count

// Maior quantidade de produtos vendidos
df.groupBy("Quantidade").agg(max("Quantidade")).show()

// Filtrando o produto FANTA e contabilizando a quantidade - Forma 1
//df.filter("Survived = 1").select("Name","Pclass").show()
df.filter("Produto == 'FANTA'").agg(sum("Quantidade")).show()

// Filtrando o produto FANTA e contabilizando a quantidade - Forma 2
//df[df["Produto"] == "FANTA"].groupby("Produto").sum()

// Verificando a quantidade mínima vendida por produto
df.groupBy("Produto").agg(min("Quantidade")).show()

// Obtendo as quantidades minima, máxima, total e média de venda dos produtos agrupados
df.groupBy("Produto").agg(min("Quantidade"), max("Quantidade"), sum("Quantidade"), mean("Quantidade")).show()

// Multiplicando a Quantidade pelo Valor_Unitario para saber o Valor_Total de vendas por produto
//df["Valor_Total"] = df.Quantidade * df.Valor_Unitario
val toDouble = udf[Double, String]( _.toDouble)

df = df.withColumn("Valor_Total", $"Quantidade" * translate($"Valor_Unitario",",",".").as("Valor_Unitario"))
df.show()

// Sumarizando Quantidade, Valor_Unitario e Valor_Total por Produto
//df.groupby("Produto").sum()
df.groupBy("Produto").agg(sum("Quantidade"),sum(translate($"Valor_Unitario", ",", ".")), sum("Valor_Total")).show()

// Contabilizando o Valor_Total por Produto, Valor_Unitario e Quantidade
//df.groupby(["Produto", "Valor_Unitario", "Quantidade"])["Valor_Total"].sum()
df.groupBy("Produto", "Valor_Total").agg(sum("Quantidade").as("Quantidade"),sum(translate($"Valor_Unitario", ",", ".")).as("Valor_Unitario"), sum("Valor_Total").as("Valor_Total")).orderBy("Produto").show()
