# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC import time
# MAGIC # Path to the file/directory
# MAGIC path = "/mnt/amerentst/output/"
# MAGIC 
# MAGIC for file_item in os.listdir(path):
# MAGIC   print(file_item)

# COMMAND ----------

dbutils.fs.ls("/mnt/amerentst/output/")

# COMMAND ----------

access_key = 'AKIA6KZQ3UPJ5KCTJAHD'
secret_key = 'siBysFH1eJlCV5LXvqLmNyUa2qJ/SJ2h1DH3cG3i'
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "shreyatest"
mount_name = "amerentst"
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
dbutils.fs.ls("/mnt/amerentst/")

# COMMAND ----------

dbutils.fs.ls("/mnt/amerentst/input/2021-06-07/SalesRecordsDelta.csv")

# COMMAND ----------

# MAGIC %python
# MAGIC import boto3
# MAGIC s3 = boto3.connect_s3()
# MAGIC bucket = s3.lookup('shreyatest')
# MAGIC for key in bucket:
# MAGIC        print (key.name, key.size, key.last_modified)

# COMMAND ----------

#s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= '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')
#s3 = boto3.resource('s3')

import boto3
s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= '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')
bucket = s3.Bucket('shreyatest')
[obj.last_modified for obj in bucket.objects.all()][:10]

# COMMAND ----------

import boto3
s3 = boto3.resource('s3')
my_bucket = s3.Bucket('shreyatest')
print(bucket.name)

# COMMAND ----------

import boto3

# Let's use Amazon S3
s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= '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')

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

# COMMAND ----------

import os
import time
import boto3
s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= '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')
my_bucket = s3.Bucket('shreyatest')
path_variable_array = []
for obj in my_bucket.objects.all():
   #print('{} , {}'.format(obj.key, obj.last_modified))
   
  path_array = obj.key.split('/')[:-1]
  path_variable = ''
  for i in path_array:
    path_variable = path_variable + i + '/'
    #print(path_variable)
  path_variable_array.append(path_variable)
  
  
print(path_variable_array)
      #+ str(obj.last_modified)
      #basetime = os.path.basetime(obj.last_modified)
      
      #if basename != '' and '.csv' in basename:
       #print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
      #if basename != '' and '.json' in basename:
       # print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
          
          

# COMMAND ----------

def createDF(file_name,file_timestamp):
  df = spark.read.format()

# COMMAND ----------

import os
basename = os.path.basename('TestDatabricks/SalesRecords.csv')
print(basename)

# COMMAND ----------

import os
import boto3
s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= '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')
my_bucket = s3.Bucket('shreyatest')

def append_timestamp(timestamp):
    name, ext = os.path.splitext(basename)
    return "{name}_{timestamp}{ext}".format(name=name, timestamp=obj.last_modified, ext=ext)
  
for obj in my_bucket.objects.all():
    #print('{} , {}'.format(obj.key, obj.last_modified))
      #+ str(obj.last_modified)
      basename = os.path.basename(obj.key)
      #print(basename)
      
      
      if basename != '' and ('.csv' in basename or '.json' in basename):
       #print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
         full_filename =  append_timestamp(obj.last_modified).replace(" ", "_")
         print(full_filename)
      
      #elif basename != '' and '.json' in basename:
        #print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
       # full_filename =  append_timestamp(obj.last_modified)
        #print(full_filename)
        
        
      #if basename != '' and '.csv' in basename:
      #   print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
      #elif basename != '' and '.json' in basename:
      #  print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
          
          

# COMMAND ----------

import os
import boto3
s3 = boto3.resource('s3',aws_access_key_id='AKIA6KZQ3UPJ5KCTJAHD',aws_secret_access_key= 'siBysFH1eJlCV5LXvqLmNyUa2qJ/SJ2h1DH3cG3i')
my_bucket = s3.Bucket('shreyatest')
  
basetimestamp_array = []
basename_array = []
for obj in my_bucket.objects.all():
    #print('{} , {}'.format(obj.key, obj.last_modified))
      #+ str(obj.last_modified)
      basetimestamp = os.path.basename(str(obj.last_modified)).replace(" ", "_")
      basename = os.path.basename(str(obj.key))
      #print(type(basetimestamp))
      if basename != '' and ('.csv' in basename or '.json' in basename):
        #print(basename+ ' ' + basetimestamp)
        basetimestamp_array.append(basetimestamp)
        basename_array.append(basename)


#print(basetimestamp_array) 
print(basename_array)


      
      #if basename != '' and ('.csv' in basename or '.json' in basename):
       #print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
       #  full_filename =  append_timestamp(obj.last_modified).replace(" ", "_")
        # print(full_filename)
      
      #elif basename != '' and '.json' in basename:
        #print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
       # full_filename =  append_timestamp(obj.last_modified)
        #print(full_filename)
        
        
      #if basename != '' and '.csv' in basename:
      #   print(basename+'_'+str(obj.last_modified).replace(" ", "_"))
      #elif basename != '' and '.json' in basename:
      #  print(basename+'_'+str(obj.last_modified).replace(" ", "_"))

# COMMAND ----------

from pyspark.sql.functions import lit
#df = spark.read.csv("dbfs:/mnt/amerentst/input/2021-06-07/SalesRecordsDelta.csv" %mount_name)

#for i in range(len(basename_array)):

df = spark.read.csv("dbfs:/mnt/amerentst/input/2021-06-06/SalesRecords.csv")
#print (df.head())
#df.show(5)

for i in range(len(basetimestamp_array)):
  df2 = df.withColumn("last_update_timestamp",lit(basetimestamp_array[i]))
#df2.show()
display(df2)


#TestDatabricks/SalesRecords.csv

# COMMAND ----------

dbutils.fs.ls("/mnt/amerentst/TestDatabricks/SalesRecords.csv")


display(df)

# COMMAND ----------

from pyspark.sql.functions import lit
#df = spark.read.csv("dbfs:/mnt/amerentst/input/2021-06-07/SalesRecordsDelta.csv" %mount_name)

#for i in range(len(basename_array)):

fixed_path = 'dbfs:/mnt/amerentst/'
#fixed_path + path_variable + basename

basetimestamp_array = []
basename_array = []
path_variable_array = []


for obj in my_bucket.objects.all():
  
  basetimestamp1 = os.path.basename(str(obj.last_modified)).replace(" ", "_")
  basename1 = os.path.basename(str(obj.key))
  
  if basename1 != '' and ('.csv' in basename1 or '.json' in basename1):
    basetimestamp_array1.append(basetimestamp1)
    basename_array1.append(basename1)
    
    path_array = obj.key.split('/')[:-1]
    path_variable = ''
    
    for i in path_array:
      path_variable = path_variable + i + '/'
      #print(path_variable)
    path_variable_array.append(path_variable)
    
    

    full_file_path = fixed_path + path_variable + basename1
    print(full_file_path)

    df = spark.read.csv(full_file_path)
    print (df.head())
    df.show(5)

    for i in range(len(basetimestamp_array1)):
      df2 = df.withColumn("last_update_timestamp",lit(basetimestamp_array1[i]))
    df2.show()
    display(df2)