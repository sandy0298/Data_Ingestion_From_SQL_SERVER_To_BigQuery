#load tables with different bq names to bq
import apache_beam as beam
import os
import argparse
import logging
import pandas as  pd
import datetime
from google.cloud import bigquery
import pytz
from oauth2client.client import GoogleCredentials
from datetime import datetime,date,timedelta
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery

class setenv(beam.DoFn): 
      def process(self,context,df_Bucket):
          import jaydebeapi
          import pandas as pd
          src1='gs://'+df_Bucket+'/JAVA_JDK_AND_JAR'
          os.system('gsutil cp '+src1+'/mssql-jdbc-10.2.1.jre8.jar /tmp/' +'&&'+ 'gsutil cp -r '+src1+'/jdk-8u202-linux-x64.tar.gz /tmp/')
          logging.info('Jar copied to Instance..')
          logging.info('Java Libraries copied to Instance..')
          os.system('mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u202-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_202/bin/java" 1 && update-alternatives --config java')
          logging.info('Enviornment Variable set.')
          return list("1")
class readandwrite(beam.DoFn): 
      def process(self, context, conn_Detail,table_list):

          import jaydebeapi
          import pandas as pd
          from datetime import datetime,date,timedelta
          from google.cloud import bigquery
          DatabaseConn=conn_Detail.split("~|*")
          database_user=DatabaseConn[0]
          database_password=DatabaseConn[1]
          database_host=DatabaseConn[2]
          database_port=DatabaseConn[3]
          database_db=DatabaseConn[4]          
          jclassname = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
          url = ("jdbc:sqlserver://"+database_host+":"+database_port+";databaseName="+database_db+";encrypt=false")
          logging.info(url)
          jars = "/tmp/mssql-jdbc-10.2.1.jre8.jar"
          libs = None
          cnx = jaydebeapi.connect(jclassname,url,{'user':database_user,'password':database_password},jars=jars)  
          logging.info('Connection Successful..') 
          cursor = cnx.cursor()
          logging.info('Query submitted to SQL Server Database..')
          logging.info('printing data')
          table_list=table_list[1:-1]
          table_list=table_list.split(",")
          for i in table_list:
            query="select * from {0}".format(i)
            sql_query = pd.read_sql(query, cnx)

            ##converting load_date from string to date##
            sql_query['load_date'] = pd.to_datetime(sql_query['load_date'])
            sql_query['load_date'] = sql_query['load_date'].dt.date

            df = pd.DataFrame(sql_query,index=None)
            client = bigquery.Client()
            table_name=['BTCH_STG_TEACHER_REGISTER','BTCH_STG_STUDENT_ACCOUNT']
            for j in table_name:
                table_id="myproject.sqlserver"+".{0}".format(j)
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    write_disposition="WRITE_TRUNCATE",
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="load_date"
                    )
                )
                job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
                
                )  # Make an API request.
                job.result()
          
def run():    
       
    try: 
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '--tables',
            required=True,
            help= ('Source table name')
            )
        parser.add_argument(
            '--dfBucket',
            required=True,
            help= ('Bucket where JARS/JDK is present')
            )
        parser.add_argument(
            '--connDetail',
            required=True,
            help= ('Source Database Connection Detail')
            )
   
        known_args, pipeline_args = parser.parse_known_args()
        
        global table_list
        table_list = known_args.tables
        global df_Bucket 
        df_Bucket = known_args.dfBucket
        global conn_Detail 
        conn_Detail = known_args.connDetail
        
        pipeline_options = PipelineOptions(pipeline_args)
        pcoll = beam.Pipeline(options=pipeline_options)
        
        logging.info("Pipeline Starts")
        dummy= pcoll | 'Initializing..' >> beam.Create(['1'])
        dummy_env = dummy | 'Setting up Instance..' >> beam.ParDo(setenv(),df_Bucket)
        readrecords=(dummy_env | 'Processing' >>  beam.ParDo(readandwrite(), conn_Detail,table_list))
        p=pcoll.run()
        logging.info('Job Run Successfully!')
        p.wait_until_finish()
    except:
        logging.exception('Failed to launch datapipeline')
        raise    


if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description=__doc__ , formatter_class=argparse.RawDescriptionHelpFormatter)
     run()
