# 🧬 AWS Glue CDC Pipeline with RDS, S3, and Lambda Trigger

This project demonstrates a complete CDC (Change Data Capture) pipeline using **AWS Glue**, **RDS**, and **S3**. It supports full snapshots and CDC updates, writes to a final output CSV in S3, and uses **Lambda** to automate Glue job execution when new files are uploaded.

---


## 📌 Table of Contents

- [📘 Architecture Overview](#-architecture-overview)
- [🧩 Setup: Connect to AWS RDS with DBeaver](#-setup-connect-to-aws-rds-with-dbeaver)
- [🔄 Set Up AWS DMS to Capture Changes](#-set-Up-AWS-DMS-to-Capture-Changes)
- [📂 S3 Bucket Structure](#-s3-bucket-structure)
- [⚙️ Lambda Trigger on S3 Upload](#️-lambda-trigger-on-s3-upload)
- [🧠 Glue Job Logic](#-glue-job-logic)
- [🚀 How to Deploy and Run](#-how-to-deploy-and-run)
- [🧭 Next Steps](#-next-steps)


---

## 📘 Architecture Overview

```plaintext
    [RDS]  →  [DMS]  →  [S3: intermediate_data/world/Person_Lewis/]
                                           ↓
                                       [Lambda Trigger]
                                           ↓
                                  [AWS Glue CDC Job]
                                           ↓
                              [S3: des_file/final_output.csv]
```
---

## 🧩 Setup: Connect to AWS RDS with DBeaver

### 🔧 Install DBeaver CE
Download and install from:  
https://dbeaver.io/download/

---

### ☁️ Create AWS RDS Instance (MySQL/PostgreSQL)
- Ensure **Public Accessibility** is enabled, or configure VPC/Subnet routing.
- Create a user with appropriate **SELECT permissions** on the database.
- Get the **RDS endpoint** from the AWS RDS Console.

---

### 🔌 Connect to RDS from DBeaver

1. Open DBeaver, click **New Database Connection**.
2. Choose your engine (Make sure what is table type stored on RDS. In my case is **MySQL**).
3. Set **Host** as your RDS endpoint:
```plaintext
my-db.1234567890.us-east-1.rds.amazonaws.com # example
```
4. Enter **Username** and **Password**.
5. Click **Test Connection** and finish setup.

Here is the schema of the table in RDS: 

```sql
CREATE TABLE person_lewis (
    PersonID INT PRIMARY KEY,
    FullName VARCHAR(100),
    City VARCHAR(100)
);
```
---

## 🔄 Set Up AWS DMS to Capture Changes

#### 1. Enable Change Capture on RDS:
- **MySQL**: Ensure these in your RDS parameter group:
- `binlog_format = ROW`
- `binlog_row_image = FULL`
- `binlog_checksum = NONE`

- Create a replication user:
 ```sql
 CREATE ROLE dms_user WITH LOGIN REPLICATION PASSWORD 'yourpass';
 GRANT SELECT ON ALL TABLES IN SCHEMA public TO dms_user;
 ```

#### 2. Create DMS Replication Instance

Go to **AWS DMS > Replication Instances** and create:

- **Task identifier**: `source-server`
- **Instance class**: `dms.t3.medium`
- **VPC**: Must match your RDS and S3
- **Public access**: Enable if needed

#### 3. Source Database Endpoint (RDS)

- **Endpoint identifier**: `source-db`
- **Endpoint type**: Source
- **Engine**: MySQL or PostgreSQL
- **Server name**: *Your RDS endpoint*
- **Port**: `3306` (MySQL) or `5432` (PostgreSQL)
- **Username/Password**: *Your DB credentials*

#### 4. Target Database Endpoint (S3)

- **Endpoint identifier**: `s3-target-lewis`
- **Endpoint type**: Target
- **Target engine**: Amazon S3
- **Amazon S3 bucket**: `de-Lewis`
- **Folder path**: `intermediate_data/world/Person_Lewis/`
- **IAM role**: Must allow `s3:PutObject` to the bucket
- **Output format**: CSV
- **Column headers**: Disabled

#### 5. Create and Start a DMS Task

Go to **AWS DMS > Database migration tasks** and create:

- **Task identifier**: `cdc-task-lewis`
- **Replication instance**: `source-server`
- **Source database endpoint**: `source-db`
- **Target database endpoint**: `s3-target-lewis`
- **Migration type**: *Migrate existing data and replicate ongoing changes*
- **selection method**: `schema` + `source table name` (use you target table) 
---

## 📦 Create S3 Bucket Structure

### 🪣 Create S3 Bucket

Create a new S3 bucket named:
```plaintext
de-Lewis # you can create any name you like. But need to be sure where the bucket located. It might influnced the setting of AWS IAM role of GLue and RDS if serivces located in different region.  
```
---
### 📁 Folder 1: `intermediate_data/`

This folder will contain all **source CSV files**, typically:
- Snapshot of table
- Or delivered changes happened to the table (like, update, delete and insert)

```plaintext
s3://de-Lewis/intermediate_data/world/Person_Lewis/
```
---

### 📁 Folder 2: `des_file/`

This folder will store:
- Glue’s **final output**: the cleaned consolidated table called **final_output.cvs**
- Any intermediate output files from Glue (since I am using spark to preprocess the data, data will be store in partitions)

Example paths:
```python
s3://de-Lewis/des_file/temp_output_20250404_123456/  # saved partions from spark
s3://de-Lewis/des_file/final_output.csv              # saved final output file
```
## ⚙️ Lambda Trigger on S3 Upload

This Lambda function automatically triggers your AWS Glue job whenever a new `.csv` file is uploaded to the `intermediate_data/world/Person_Lewis/` folder in your S3 bucket.

---

### 🧠 Lambda Function Code (Python)

```python
import json
import boto3
import urllib.parse

def lambda_handler(event, context): # the lambda fucntion called: trigger-glue-lewis
    glue = boto3.client('glue')
    
    # Get bucket and file info from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    file_name = object_key.split('/')[-1]

    response = glue.start_job_run(
        JobName='consolidate-employee-data-lewis',
        Arguments={
            '--dst_bucket_name': bucket_name,
            '--file_name': file_name
            # Add more if needed
        }
    )
    print(
        {
    'statusCode': 200,
    'body': json.dumps({
        'message': 'Glue job triggered successfully',
        'bucket': bucket_name,
        'file': file_name,
        'jobRunId': response["JobRunId"]
    })
}
    )

    return {
    'statusCode': 200,
    'body': json.dumps({
        'message': 'Glue job triggered successfully',
        'bucket': bucket_name,
        'file': file_name,
        'jobRunId': response["JobRunId"]
    })
}
```

### S3 Event Trigger Setup

To connect S3 with Lambda:

- Go to your S3 bucket in the AWS Console
- Navigate to Properties → Event Notifications
- Click Create event notification

Set:
```plaintxt
Event name: glue-trigger-on-upload
Event type: PUT
Prefix: intermediate_data/world/Person_Lewis/
Suffix: .csv
Destination: trigger-glue-lewis
```
---

## 🧠 Glue Job Logic

The AWS Glue job is responsible for reading either **snapshot** or **CDC files**, applying data transformations, and writing the consolidated result to a final output file in S3.

---

### 🔍 File Type Detection

- The input file is read from:
```
s3://<bucket>/intermediate_data/world/Person_Lewis/<file_name>
```

- If the file has:
- **3 columns**: treated as a **snapshot**
- **4 columns**: treated as **CDC (Change Data Capture)**

---

### 🧾 Schema Expectations

| File Type | Columns                                 |
|-----------|------------------------------------------|
| Snapshot  | `PersonID`, `FullName`, `City`           |
| CDC       | `Op`, `PersonID`, `FullName`, `City`     |

Where `Op` can be:
- `I` → Insert
- `U` → Update
- `D` → Delete

---

### ⚙️ Glue Logic

1. **For Snapshot Files**:
 - The final output is completely replaced with the snapshot data.

2. **For CDC Files**:
 - Existing data is read from:
   ```
   s3://<bucket>/des_file/final_output.csv
   ```
 - Records are filtered by operation type:
   - `D` → Deletes rows with matching `PersonID`
   - `U` → Replaces updated records
   - `I` → Adds new records
 - The updated dataset is merged and written out.

---

### 📁 Output Location

- A temporary folder is created during execution:
```
s3://<bucket>/des_file/temp_output_<timestamp>/
```

- The output CSV is saved as a single-part file and copied to:
```
s3://<bucket>/des_file/final_output.csv
```


---

### 🧹 Cleanup

- After copying, the temporary folder is automatically deleted to save storage.

---

### 🧠 Glue Job Parameters

| Parameter         | Description                         |
|------------------|-------------------------------------|
| `--dst_bucket_name` | Target S3 bucket name             |
| `--file_name`     | Name of the CSV file to process     |

---

### ✅ Example Glue Trigger

The Glue job can be triggered manually or via Lambda (e.g., when a new file arrives in S3).

The full implementation is in [glue.py](glue.py). Check it out. 


## 🚀 How to Deploy and Run

This section explains how to deploy the CDC pipeline and run the full end-to-end process using RDS, DMS, Lambda, and AWS Glue.

---

### ✅ Step-by-Step Deployment Guide

#### 1. Export or Replicate Source Data to S3
Please follows the method:

- **Manual Upload or change**:
  - start your **DMS** service.
  - Create or change your target table using **DBeaver**.
  - have a cup of tea. 
  - check you destination folder and see the new update table. 

## 🧭 Next Steps

Here are a few enhancements to improve observability, performance, and extensibility of your CDC pipeline:

---

### 📁 1. Store AWS Glue Logs in S3

To analyze logs later using Athena or external tools:

#### ✅ Steps:

1. Go to the **AWS Glue Console**
2. Open your job → Click **Edit**
3. Scroll to **Monitoring Options**
4. Enable:
   - ✅ **Continuous logging**
   - ✅ **Job bookmark option** (optional)
5. Set a log destination path in S3, e.g.:

### 📦 2. Register Output as Glue Table

- Register `des_file/final_output.csv` as an external table in the Glue Data Catalog
- This enables querying the result with **Athena** or **Redshift Spectrum**

---

### 🔄 3. Convert Final Output to Parquet

- Use Glue to convert `final_output.csv` to **Parquet**
- Benefits: faster querying, compression, schema evolution support

