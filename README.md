# Data Ingestion from Sql Server to BigQuery

## About
This Project is all about ingesting multiple tables from SQL SERVER to BIGQUERY using CLoud Dataflow and orchestrating it through Cloud Composer. An Email Notification Mechanism is aloso defiined using GMAIL API to Trigger Email notification when data load activity is completed. the partitioning of the tables are handled in the dataflow pipeline.

## Toolbox 🧰
<img src="https://lh6.googleusercontent.com/1MICxjbrbRPtEnzE54g2shaMRD2RocCIcuSOrqwaqryObCR6IrsXNb3Sd5MjBBwmoLeVcgVu_SE3vw-IbRA24SFhH4IT1xppVuuNGodDtFEykgD0Cw1vB2jITTsOgBNHvWfw27icmMs30SYgWQ" width="200" alt="GCP DTAFLOW" height="70"/>&emsp; 
<img src="https://miro.medium.com/max/600/1*HEzofakm1-c4c_Qn4zjmnQ.jpeg" width ="170" height="75" alt="Apache Beam"/>&emsp;
<img src ="https://cxl.com/wp-content/uploads/2019/10/google-bigquery-logo-1.png" width="170" height="100" alt="Google Big Query"/> &emsp;
<img src ="https://www.python.org/static/community_logos/python-logo-master-v3-TM-flattened.png" width="170" height="100" alt="Python"/> &emsp;
<img src = "https://th.bing.com/th/id/OIP.0XChTiQy-sBUWPSLVMsy9AHaEo?pid=ImgDet&rs=1" width="170" height="100" alt="cloud composer"/> &emsp;
<img src = "https://e7.pngegg.com/pngimages/170/924/png-clipart-microsoft-sql-server-microsoft-azure-sql-database-microsoft-text-logo.png" width="170" height="100" alt="sql server"/> &emsp;

## Architecture Diagram

<img src ="https://github.com/sandy0298/Data_Ingestion_From_SQL_SERVER_To_BigQuery/blob/6ca1f29972d584f5b1fde6d92f39b1324ebd3c22/sql_server.png" width="800" height="600" alt="architecture"/> &emsp;

### Code structure
```
├── Home Directory
|     ├── SQL_Server_Dataflow
|     |     ├── sql_server_dataflow_job.py
├──Setup.py
 
```

## Installation Steps and deployment process
<b>1.</b>For running Dataflow We need to install Java Jdk 8 on the master node. For that we are making use of GCS Bucket to hold the JDk 8 Package and installing the dependency at run time on the master Node.<br>
<b>2.</b>We are making use of <b> Setup.py </b> file to pass on the list of all the dependency that needs to be installed at run time on the worker nodes.
A better production approach could be to make a custom container having all the required dependency installed and will be provided to the dataflow job at run time which will increases the job efficiency as need to install dependency seprately on each worker node during up scalling will vanquish. <br>
<b>3.</b>we are using GMAIL API to send email notification through composer and the orchestration is done through composer.<br>



