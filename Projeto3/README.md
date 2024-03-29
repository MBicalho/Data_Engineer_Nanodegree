<h3 align="center">Cloud Data Warehouse</h3>
<p align="center">
 Udacity Data Engineer Nanodegree Course Project 3
 <br />
</p>


# About the Project

In this project, we learned how to use Amazon Redshift to store a database and also how to use it to create Data Warehouses, it was also necessary to create an ETL in Python responsible for loading data from S3 to staging tables in Redshift and then loading data from staging tables for analysis tables in Redshift.

Nesse projeto, aprendemos como utilizar o Amazon Redshift para armazenar um banco de dados e também como o utilizar para criar Data Warehouses, foi necessário também criar um ETL em Python responsável por carregar dados do S3 para tabelas de preparo no Redshift e depois carregar dados de tabelas de preparo para tabelas de análise no Redshift.

# Project Description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Uma startup de streaming de música, Sparkify, aumentou sua base de usuários e banco de dados de músicas e deseja mover seus processos e dados para a nuvem. Seus dados residem no S3, em um diretório de logs JSON sobre a atividade do usuário no aplicativo, bem como em um diretório com metadados JSON nas músicas em seu aplicativo.

Como engenheiro de dados, você tem a tarefa de criar um pipeline de ETL que extraia seus dados do S3, os organiza no Redshift e os transforma em um conjunto de tabelas dimensionais para que a equipe de análise continue encontrando insights sobre quais músicas seus usuários estão ouvindo . Você poderá testar seu banco de dados e pipeline de ETL executando consultas fornecidas pela equipe de análise do Sparkify e comparar seus resultados com os resultados esperados.

# Tools Used

* Python
* PostgreSQL
* Jupyter notebooks

# Datasets

Two datasets residing on S3 were used.
* Song data: ```s3://udacity-dend/song_data```
* Song data: ```s3://udacity-dend/log_data```

Log data json path: ```s3://udacity-dend/log_json_path.json```

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```


Foi utlizado dois conjuntos de dados que residem no S3.
* Song data: ```s3://udacity-dend/song_data```
* Song data: ```s3://udacity-dend/log_data```

Log data json path: ```s3://udacity-dend/log_json_path.json```

O primeiro conjunto de dados é um subconjunto de dados reais do [Million Song Dataset](http://millionsongdataset.com/). Cada arquivo está no formato JSON e contém metadados sobre uma música e o artista dessa música. Os arquivos são particionados pelas três primeiras letras do ID da faixa de cada música. Por exemplo, aqui estão os caminhos de arquivo para dois arquivos neste conjunto de dados.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
E abaixo está um exemplo da aparência de um único arquivo de música, TRAABJL12903CDCF1A.json.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

O segundo conjunto de dados consiste em arquivos de log no formato JSON gerados por este simulador de eventos com base nas músicas do conjunto de dados acima. Eles simulam os logs de atividade do aplicativo de um aplicativo de streaming de música imaginário com base nas configurações.

Os arquivos de log no conjunto de dados com os quais você trabalhará são particionados por ano e mês. Por exemplo, aqui estão os caminhos de arquivo para dois arquivos neste conjunto de dados.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

# Data Modeling

The modeling will be done thinking about the queries that we will need to perform first, after that we will create our tables.

A modelagem será feita pensando sobre as consultas que precisaremos realizar primeiro, após isso criaremos nossas tabelas.



# Project Structure

|File/Folder| File/Folder Description |
| --- | --- |
| create_cluster.ipynb | The script responsible for creating the cluster  |
| sql_queries.py | The file responsible for SQL inserts, create and copy |
| images | Where it is stored as images used in the project |
| create_tables.py | File Python with the command for create the tables |
| etl.py | File Python with the etl from S3 buckets to the tables |
| Test.ipynb | Script to test if data was entered correctly |
| dwh.cfg | File with the configurations of the AWS |
| README.md | Readme |

# How to Execute

Clone the repository on your machine run through the command.
```git clone https://github.com/MBicalho/Data_Engineer_Nanodegree.git```

# Tools to Execute the project

* Python
* PostgreeSQL
* Pandas, json, boto3, configparser, psycopg2 libraries
* Jupyter notebook
* AWS Services

# Step by Step
*Configure

First configure the file ```dwh.cfg```

* Run the file
```create_cluster.ipynb```
run until the comment 
```#The cells below, is just for exclude the cluster```
the cells below that don't run yet

After this, execute
```create_tables.py```
and
```etl.py```
after this, execute the test
```Test.ipynb```


# Contact
Matheus Bicalho [mbicalho.freitas@gmail.com]
Linkedin: [https://www.linkedin.com/in/matheus-bicalho-0a5835205/]
