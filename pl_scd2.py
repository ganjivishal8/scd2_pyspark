
import pyspark
import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

#Global Connection Variables
db_name=input('Enter database')
url=f"jdbc:postgresql://localhost:5432/{db_name}"
source_name='source_scd'
dest_name='dest_scd'
username=input('Enter Username')
password=input('Enter password')

#Globally Used Variables
def_timestamp="9999-12-31 23:59:59"
key_list=["id"]
type2_cols=["company","role"]
scd2_cols = ["effective_date","expiration_date","flag"]
DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

class Scd():
    def __init__(self) -> None:
        pass
        
    def readfromDb(self,url,table_name,username,password):
        df=spark.read.format("jdbc"). \
                    option("url", url). \
                    option("driver", "org.postgresql.Driver"). \
                    option("dbtable", table_name). \
                    option("user", username). \
                    option("password", password). \
                    load()
        return df
    
    def writetoDb(self,df,url,table_name,username,password):
        df.write.format("jdbc"). \
                    option("url", url). \
                    option("driver", "org.postgresql.Driver"). \
                    option("dbtable", table_name). \
                    option("user", username). \
                    option("password", password). \
                    mode("overwrite").save()
        return
    
    def rename(self,df,suffix,append):
        if append:
            new_cols=list(map(lambda x:x+suffix,df.columns))
        else:
            new_cols=list(map(lambda x:x.replace(suffix,""),df.columns))
        
        return df.toDF(*new_cols)

    def getHash(self,df,key_list):
        columns = [col(column) for column in key_list]
        if columns:
            return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
        else:
            return df.withColumn("hash_md5", md5(lit(1)))
        
    def max_skey(self,df):
        max_sk = df.agg({"sk_id": "max"}).collect()[0][0]
        return max_sk
    
    def merge_action(self,df_history_open_hash):
        df_merged = df_history_open_hash\
                .join(df_current_hash, col("id_current") ==  col("id_history"), how="full_outer")\
                .withColumn("Action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                .when(col("id_current").isNull(), 'DELETE')\
                .when(col("id_history").isNull(), 'INSERT')\
                .otherwise('UPDATE'))
        return df_merged
    
    def no_change(self,df_merged):
        df_nochange=self.rename(df_merged.filter(col("Action")=='NOCHANGE'), suffix="_history", append=False).\
                    select(df_history_open.columns)
        return df_nochange
    
    def inserted(self,df):
        df_insert = self.rename(df.filter(col("Action") == 'INSERT'), suffix="_current", append=False)\
                        .select(df_current.columns)\
                        .withColumn("effective_date",date_format(current_timestamp(),DATE_FORMAT))\
                        .withColumn("expiration_date",date_format(lit(def_timestamp),DATE_FORMAT))\
                        .withColumn("row_number",row_number().over(window_spec))\
                        .withColumn("sk_id",col("row_number")+ max_sk)\
                        .withColumn("flag", lit(True))\
                        .drop("row_number")
        return df_insert
    
    def deleted(self,df_merged,df_history_open):
        df_deleted = self.rename(df_merged.filter(col("action") == 'DELETE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", date_format(current_timestamp(),DATE_FORMAT))\
                .withColumn("flag", lit(False))
        return df_deleted
    
    def updated(self,df_merged,df_history_open):
        df_update = self.rename(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", date_format(current_timestamp(),DATE_FORMAT))\
                .withColumn("flag", lit(False))\
            .unionByName(
            self.rename(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                .select(df_current.columns)\
                .withColumn("effective_date",date_format(current_timestamp(),DATE_FORMAT))\
                .withColumn("expiration_date",date_format(lit(def_timestamp),DATE_FORMAT))\
                .withColumn("row_number",row_number().over(window_spec))\
                .withColumn("sk_id",col("row_number")+ max_sk)\
                .withColumn("flag", lit(True))\
                .drop("row_number")
                )
        return df_update
    

if __name__=="__main__":
    scd=Scd()

    spark=SparkSession.builder.getOrCreate()
    
    dest_table=spark.read.format('jdbc'). \
     options(
         url=url,
         dbtable='information_schema.tables',
         user=username,
         password=password,
         driver='org.postgresql.Driver'). \
     load().filter(col('table_name')== dest_name).collect()


    if dest_table:
        df_current=scd.readfromDb(url,source_name,username,password)
        df_current.show()

        #history_df is incremental
        df_history=scd.readfromDb(url,"dest_scd",username,password)
        df_history.show()

        max_sk=scd.max_skey(df_history)

        # filter out open records from df_history
        # we don't need to do any changes in closed records
        df_history_open = df_history.where(col("flag"))
        df_history_closed = df_history.where(col("flag")==lit(False))

        #Creating Unique hash for current dataframe and History Dataframe
        df_history_open_hash = scd.rename(scd.getHash(df_history_open, type2_cols), suffix="_history", append=True)
        df_current_hash = scd.rename(scd.getHash(df_current, type2_cols), suffix="_current", append=True)

        #Adding an Action Column to Dataframe
        df_merged=scd.merge_action(df_history_open_hash)
        window_spec=Window.orderBy("id")
        #Collecting No Change records
        df_nochange=scd.no_change(df_merged)

        #Collecting Inserted records
        df_insert=scd.inserted(df_merged)
        #max_sk=scd.max_skey(df_insert)
        if df_insert.isEmpty():
            max_sk = df_merged.agg({"sk_id_history": "max"}).collect()[0][0]
        else:
            max_sk = df_insert.agg({"sk_id": "max"}).collect()[0][0]

        #Collecting Deleted Records
        df_deleted=scd.deleted(df_merged,df_history_open)

        #Collecting Updated Records
        df_update=scd.updated(df_merged,df_history_open)

        #Merging All the records to final dataframe
        df_final = df_history_closed\
            .unionByName(df_nochange)\
            .unionByName(df_insert)\
            .unionByName(df_deleted)\
            .unionByName(df_update)
        
        #Writing Dataframe to destination scd table
        scd.writetoDb(df_final,url,"dest_scd",username,password)

        #Creating copy of dest_scd as Incremental
        # incremental_df=scd.readfromDb(url,"dest_scd",username,password)
        # scd.writetoDb(incremental_df,url,"incremental",username,password)

    
    
    else:
        #Initial Load
        #Intially we have data in source table
        src_df=spark.read.csv(r'D:\pyspark practice\Data_Processing\scd2\employee.csv',header=True)
        src_df=src_df.withColumn('id',col('id').cast('int'))
        src_df.show()
        scd.writetoDb(src_df,url,"source_scd",username,password)

        ##Taking current dataframe and adding necessary columns to to store in destination table

        current_df=scd.readfromDb(url,"source_scd",username,password)

        window_spec=Window.orderBy('id')

        current_df=current_df.withColumn('sk_id',row_number().over(window_spec)).\
                    withColumn('effective_date',date_format(current_timestamp(),DATE_FORMAT)).\
                    withColumn('expiration_date',date_format(lit(def_timestamp),DATE_FORMAT)).\
                    withColumn('flag',lit(True))

        current_df.show()

        #Writing Dataframe to destination scd table
        #Creating copy of dest_scd as Incremental
        scd.writetoDb(current_df,url,"dest_scd",username,password)
        # scd.writetoDb(current_df,url,"incremental",username,password)


