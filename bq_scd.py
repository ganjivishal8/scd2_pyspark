
import pyspark
import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from google.cloud import bigquery

#Global Connection Variables
GCP_PROJECT_ID = "data-project-406509"
dataset_name = "vishal_db"
BIGQUERY_TABLE_NAME = ""
TEMPORARY_BUCKET_NAME = "vish_bucket1"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

sa_path='key.json'

#Globally Used Variables

def_timestamp="9999-12-31 23:59:59"
key_list=["id"]
type2_cols=["company","role"]
scd2_cols = ["effective_date","expiration_date","flag"]
DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

class Scd():
    def __init__(self) -> None:
        pass
    
    def writetoDb(self,dataframe: DataFrame, dataset_name, table_name):
        dataframe.write.format('bigquery') \
            .option("table", f"{GCP_PROJECT_ID}.{dataset_name}.{table_name}") \
            .option('parentProject', GCP_PROJECT_ID) \
            .option("temporaryGcsBucket", TEMPORARY_BUCKET_NAME) \
            .mode("overwrite") \
            .save()
    
    def readfromDb(self,dataset_name, table_name):
        return spark.read.format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{dataset_name}.{table_name}") \
            .load()
    
    def tablesList(self,dataset_id,req_table):
        list_objects = list(client.list_tables(dataset_id))
        l=[]
        for i in list_objects:
            # print(i.table_id)
            l.append(i.table_id)
        return True if req_table in l else False
    
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
        
    # def max_skey(self,df):
    #     max_sk = df.agg({"sk_id": "max"}).collect()[0][0]
    #     return max_sk
    
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

    dataset_id = f'{GCP_PROJECT_ID}.{dataset_name}'
    client = bigquery.Client()

    dest_table=scd.tablesList(dataset_id,"dest_scd")

    if dest_table: 


        #Creating current dataframe from source table
        df_current=scd.readfromDb(dataset_name,"source_scd")
        df_current.show()

        #history_df is staging history table
        df_history=scd.readfromDb(dataset_name,"dest_scd")
        df_history.show()

        max_sk = df_history.agg({"sk_id": "max"}).collect()[0][0]

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
        # max_sk=scd.max_skey(df_insert)
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
        scd.writetoDb(df_final,dataset_name,"dest_scd")

        #Creating copy of dest_scd as staging
        # staging_df=scd.readfromDb(dataset_name,"dest_scd")
        # scd.writetoDb(staging_df,dataset_name,"staging")
        
    
    else:
        #Initial Load
        #Intially we have data in source table
        src_df=spark.read.csv(r'D:\pyspark practice\Data_Processing\scd2\employee.csv',header=True)
        src_df=src_df.withColumn('id',col('id').cast('int'))
        src_df.show()
        scd.writetoDb(src_df,dataset_name,"source_scd")

        ##Taking current dataframe and adding necessary columns to to store in destination table

        current_df=scd.readfromDb(dataset_name,"source_scd")

        window_spec=Window.orderBy('id')

        current_df=current_df.withColumn('sk_id',row_number().over(window_spec)).\
                    withColumn('effective_date',date_format(current_timestamp(),DATE_FORMAT)).\
                    withColumn('expiration_date',date_format(lit(def_timestamp),DATE_FORMAT)).\
                    withColumn('flag',lit(True))

        current_df.show()

        #Writing Dataframe to destination scd table
        #Creating copy of dest_scd as staging
        scd.writetoDb(current_df,dataset_name,"dest_scd")
        # scd.writetoDb(current_df,dataset_name,"staging")

