from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

# Инициализация SparkSession
spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

# Пример данных для продуктов
products_data = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D")
]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

# Пример данных для категорий
categories_data = [
    (1, "Category X"),
    (2, "Category Y"),
    (3, "Category Z")
]
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

# Пример данных для связей между продуктами и категориями
product_category_data = [
    (1, 1),
    (1, 2),
    (2, 1),
    (3, 3)
]
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

# Левое соединение продуктов и связей
product_category_join = products_df.join(product_category_df, on="product_id", how="left")

# Левое соединение с категориями для получения имен категорий
result_df = product_category_join.join(categories_df, on="category_id", how="left")

# Отфильтруем пары «Имя продукта – Имя категории»
product_category_pairs_df = result_df.select("product_name", "category_name").filter(col("category_name").isNotNull())

# Найдем продукты, у которых нет категорий
products_no_category_df = result_df.select("product_name").filter(col("category_name").isNull()).distinct()

# Показать результаты
print("Пары «Имя продукта – Имя категории»:")
product_category_pairs_df.show()

print("Продукты, у которых нет категорий:")
products_no_category_df.show()
