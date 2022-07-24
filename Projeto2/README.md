<h3 align="center">Data Modeling ETL with PostgreSQL</h3>
<p align="center">
 Udacity Data Engineer Nanodegree Course Project 1
 <br />
</p>


# About the Project

In this project, we will model a database based on a music streaming scenario, the modeling will be done using NoSQL Apache Cassandra and we will create an ETL using Python, we will define three tables for the types of queries that will be performed, and an ETL pipeline will be written that will transfer the data from CSV files in local directories to our database.

Neste projeto, será feito a modelagem de um banco de dados baseado em um cenário de streaming de música, a modelagem será feita a partir do NoSQL Apache Cassandra e criaremos uma ETL usando Python, defineremos três tabelas para os tipos de consultas que serão realizadas, e será escrito um pipeline ETL que irá transferir os dados de arquivos CSV em diretórios locais para o nosso banco de dados.

# Project Description

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

Uma startup chamada Sparkify quer analisar os dados coletados em músicas e atividades do usuário em seu novo aplicativo de streaming de música. A equipe de análise está particularmente interessada em entender quais músicas os usuários estão ouvindo. Atualmente, não há uma maneira fácil de consultar os dados para gerar os resultados, pois os dados residem em um diretório de arquivos CSV sobre a atividade do usuário no aplicativo.

Eles gostariam que um engenheiro de dados criasse um banco de dados Apache Cassandra que pudesse criar consultas sobre dados de reprodução de músicas para responder às perguntas, e gostaria de trazer você para o projeto. Sua função é criar um banco de dados para esta análise. Você poderá testar seu banco de dados executando consultas fornecidas pela equipe de análise do Sparkify para criar os resultados.

# Tools Used

* Python
* NoSQL Apache Cassandra
* Jupyter notebooks

# Datasets

Para este projeto, você trabalhará com um conjunto de dados: ```event_data```. O diretório de arquivos CSV particionados por data. Aqui estão exemplos de caminhos de arquivo para dois arquivos no conjunto de dados:


For this project, you will work with one dataset: ```event_data```. The directory of date-partitioned CSV files. Here are example file paths for two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```



# Data Modeling

The modeling will be done thinking about the queries that we will need to perform first, after that we will create our tables.

A modelagem será feita pensando sobre as consultas que precisaremos realizar primeiro, após isso criaremos nossas tabelas.

![ERD](./images/Screenshot_44.png)


# Project Structure

|File/Folder| File/Folder Description |
| --- | --- |
| data | The root of project, where are the datas of songs and logs |
| images | Where it is stored as images used in the project |
| create_tables.py | File used to connect to the database, and call the create, insert and drop functions of the created tables |
| sql_queries.py | File used to store the creation, insertion and drop codes of the tables used in the project. |
| etl.ipynb | Template to follow for storing song_data and log_data files and inserting them into tables. |
| test.ipynb | Template to validate the inserts in the tables. |
| etl.py | File to store song_data and log_data files and insert them into tables |
| README.md | Readme |

# How to Execute

Clone the repository on your machine run through the command.
```git clone https://github.com/MBicalho/Data_Engineer_Nanodegree.git```

# Tools to Execute the project

* Python
* PostgreSQL
* Pandas, psycopg2 and glob libraries

# Step by Step

* Run the file ```create_tables.py```
```python create_tables.py```
* Run the file ```etl.py```
```python etl.py```
* Test with the ```test.ipynb```

# Contact
Matheus Bicalho [mbicalho.freitas@gmail.com]
Linkedin: [https://www.linkedin.com/in/matheus-bicalho-0a5835205/]
