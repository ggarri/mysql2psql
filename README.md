# Migrate Mysql db to Postgresql (by rules) 

## OS Dependences
### Install pip
    sudo apt-get install python-pip python-dev build-essential python-psycopg2 python-mysqldb libpq-dev libmysqlclient-dev
    
## Environment dependences
### Install python libraries and vendors
    sudo bash ./bin/install_requirements.sh
    
## How to use
### Step1: Set up db config
Set up your database configuration on "./config/parameters.json"
* `mysql`: Mysql connection values
* `psql`: Postgres connection values
* `threads`: In case of 'all-databases', you can define the number of threads to run in parallel (Max. number of CPUs). Non parallel 0
* `prefix`: In case of 'all-databases', it filters every database which prefix is the defined here. Otherwise use false
* `v1_schema_name`: If you want to migrate old schema onto a separated postgres schema, its name is defined here. Otherwise use false

### Step2: Version schema names
Set up your schema names for version1 and version2 on "./config/parameters.json"

### Step2: Define model rules you want to modified
* Open "./rules/schema_changes.json"
* Define your own schema rules on it. These rules are going to be used to redefine the new db structure, in case of not including any rules to a table or column, they will be migrated as it is in Mysql 

### Step3: Define Postgresql conversion rules from Mysql ones
* Open "./rules/mysql_to_psql.json"
* Define MySQL keys to Postgres, most of rules were already defined by default, but there might be some more missing

### Step4: Define data convertion 
* Open "./rules/mysql_raw_dump.json"
* Define data conversion according to its type, YOU might prefer to define different data conversion depending of your own model. Functions for conversion are defined in `dumperAuxFuncs.py`, feel free to add your own customized ones.

## Generate Postgresql schema
 
#### Mode1: From given Mysql db name
    $ python main.py {db_name}
    
#### Mode2: From given Mysql db name and list of tables
    $ python main.py {db_name} [{table_name1} {table_name2} ..]


## Generate Postgres Data from generated files
### Mode1: Manually
#### Create tables
    psql -h server -d database_name -U username < ./output/psql_tables.sql
#### Insert data
    psql -h server -d database_name -U username < ./output/psql_data.sql
#### Create views
    psql -h server -d database_name -U username < ./output/psql_views.sql

## Mode2: Single command
    $ bash ./bin/migrate.sh [-p {port}] -U {username} -d {database} -Wf {password}

========================

### Notes
#### Output files generated
* "mysql_schema.json": Original Mysql schema exported in Json format
* "mysql_schema_v2.json": Mysql schema after model rules where applied
* "mysql_data.sql": INSERT INTO statement in mysql

* "psql_schema.json": Postgres schema 
* "psql_tables.sql": CREATE TABLE statements, generated from psql_schema. 
* "psql_data.sql": INSERT INTO statements, generated from psql_schema. Raw data will be allocated under ./table folder