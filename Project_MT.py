#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import *

import pyspark.sql.functions as func


# In[5]:


import warnings
warnings.filterwarnings('ignore')


# In[3]:


import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# In[6]:


#Initialize Spark session with:
spark = SparkSession.builder.appName("zillow").getOrCreate()


# In[7]:


zillow = spark.read.csv('zillow.csv.gz', inferSchema=True, header=True)
zillow.cache()
print(f'{zillow.count():,}')


# In[8]:


zillow.printSchema()


# In[9]:


zillow.limit(5).toPandas()


# 1. Extract number of bedrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bedrooms.

# In[10]:


#Python UDF
def _bedrooms_UDF(x):
    bedrooms = x.split(" ")[0].strip()
    if bedrooms.isdigit(): 
        return int(bedrooms)
    else:
        return None
    

#To Spark
bedrooms_UDF = udf(_bedrooms_UDF, IntegerType())
zillow = zillow.withColumn("bedrooms", bedrooms_UDF(func.col("facts and features"))).cache()
zillow.count()
zillow.limit(5).toPandas()


# 2. Extract number of bathrooms. You will implement a UDF that processes the `facts_and_features` column and extracts the number of bathrooms.

# In[11]:


#Python UDF
# '2 bds, 1.0 ba ,1057 sqft',
# '0 bds, None ba ,9999 sqft',
def _bathrooms_UDF(x):
    bathrooms = x.replace(" ", "").split(",")[1].replace("ba", "").replace(".0", "")
    
    #return bathrooms
    if bathrooms.isdigit():
        return int(float(bathrooms))
    else:
        return None

#To Spark
bathrooms_UDF = udf(_bathrooms_UDF, StringType())
zillow = zillow.withColumn("bathrooms", bathrooms_UDF(func.col("facts and features"))).cache()
zillow.count()
zillow.limit(5).toPandas()


# 3. Extract sqft. You will implement a UDF that processes the `facts_and_features` column and extracts the sqft.

# In[12]:


#Python UDF
def _sqft_UDF(x):
    sqft = x.split(",")[2].split(" ")[0].strip()
    if sqft.isdigit():
        return int(sqft)
    else:
        return None

#To Spark
sqft_UDF = udf(_sqft_UDF, IntegerType())
zillow = zillow.withColumn("sqft", sqft_UDF(func.col("facts and features"))).cache()
zillow.count()
zillow.limit(5).toPandas()


# 4. Extract type. You will implement a UDF that processes the `title` column and returns the type of the listing (e.g. condo, house, apartment)

# In[13]:


#Python UDF
def _type_UDF(x):
    house_type = x.split(" ")[0].strip()
    if house_type.isalpha():
        return house_type
    else:
        return None

#To Spark
type_UDF = udf(_type_UDF, StringType())
zillow = zillow.withColumn("house_type", type_UDF(func.col("title"))).cache()
zillow.count()
zillow.limit(5).toPandas()


# 5. Extract offer. You will implement a UDF that processes the `title` column and returns the type of offer. This can be `sale`, `rent`, `sold`, `forclose`.

# In[14]:


zillow.select("title").distinct().toPandas()


# In[15]:


#Python UDF
def _type_UDF(x):
    types = ['sale', 'rent', 'sold', 'foreclosure']
    for tp in types:
        if x.lower().find(tp) != -1:
            return tp
    return None


#To Spark
type_UDF = udf(_type_UDF, StringType())
zillow = zillow.withColumn("offer", type_UDF(func.col("title"))).cache()
zillow.count()
zillow.limit(5).toPandas()


# 6. Filter out listings that are not for sale.

# In[16]:


zillow.filter(func.col("offer") == "sale").limit(5).toPandas()


# 7. Extract price. You will implement a UDF that processes the `price` column and extract the price. Prices are stored as strings in the CSV. This UDF parses the string and returns the price as an integer.

# In[17]:


#Python UDF
import re
def _price_UDF(x): #x = row element
    price = re.sub("[^0-9]", "", x)
    return float(price)

#To Spark
offer_UDF = udf(_price_UDF, DoubleType())
zillow = zillow.withColumn("newprice", offer_UDF(func.col("price"))).cache()
zillow.limit(5).toPandas()


# 8. Filter out listings with more than 10 bedrooms

# In[18]:


zillow.filter(func.col("bedrooms") <= 10).limit(5).toPandas()


# 9. Filter out listings with price greater than 20000000 and lower than 100000

# In[19]:


zillow.filter((func.col("newprice") < 20000000) & (func.col("newprice") > 100000)).limit(5).toPandas()


# 10. Filter out listings that are not houses

# In[20]:


zillow.filter(func.col("house_type") == "House").limit(5).toPandas()


# 11. Calculate average price per sqft for houses for sale grouping them by the number of bedrooms.

# In[21]:


df = zillow.filter(func.col("offer") == "sale")

average = spark.createDataFrame(df.groupBy(df.bedrooms).avg().collect())
average = average.withColumn('Prices', func.col('avg(newprice)').cast(DecimalType(18, 2))).cache()
average.select('bedrooms','Prices').toPandas()

