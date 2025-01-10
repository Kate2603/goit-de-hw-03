from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# Створюємо сесію Spark
spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

# Завантажуємо дані
users_df = spark.read.csv('users.csv', header=True, inferSchema=True)
purchases_df = spark.read.csv('purchases.csv', header=True, inferSchema=True)
products_df = spark.read.csv('products.csv', header=True, inferSchema=True)

# Очищаємо дані
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Об'єднуємо purchases_df з products_df для отримання категорії продуктів
purchases_with_category = purchases_df.join(products_df, "product_id")

# 3. Визначаємо загальну суму покупок за категорією продуктів
total_purchases_by_category = purchases_with_category.groupBy("category") \
    .agg(round(sum(col("quantity") * col("price")), 2).alias("total_spent")) \
    .orderBy("total_spent", ascending=False)

# Виводимо заголовок і результати
print("Загальна сума покупок за категорією продуктів:")
total_purchases_by_category.show()

# 4. Визначаємо суму покупок за категорією продуктів для вікової категорії від 18 до 25 включно
young_users = users_df.filter((col("age") >= 18) & (col("age") <= 25))
young_purchases = purchases_df.join(young_users, "user_id")
young_purchases_with_category = young_purchases.join(products_df, "product_id")

total_young_purchases_by_category = young_purchases_with_category.groupBy("category") \
    .agg(round(sum(col("quantity") * col("price")), 2).alias("total_spent")) \
    .orderBy("total_spent", ascending=False)

# Виводимо заголовок і результати
print("Сума покупок за категорією продуктів для вікової категорії 18-25:")
total_young_purchases_by_category.show()

# 5. Визначаємо частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років
total_young_spent = young_purchases_with_category.agg(sum(col("quantity") * col("price"))).first()[0]

young_purchases_share = total_young_purchases_by_category.withColumn("share", 
    round((col("total_spent") / total_young_spent) * 100, 2))  # Округлення до 2 знаків після коми

# Виводимо заголовок і результати
print("Частка покупок за категорією товарів для вікової категорії 18-25:")
young_purchases_share.show()

# 6. Вибираємо 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років
top_categories = young_purchases_share.orderBy(col("share").desc()).limit(3)

# Виводимо заголовок і результати
print("Топ-3 категорії продуктів з найвищим відсотком витрат:")
top_categories.show()

# Закриваємо сесію Spark
spark.stop()