# Data ingest using Apache NiFi
An introductory hands-on class to learn basic concepts of data flow, which will allow you to collect and transport data from numerous sources including real-time events, data from external sources, structured and unstructured data.

### Use Cases:
* ingest data from database
    - table dump
    - query dump 
    - incremental
    - change data capture
* ingest data from external sources 
    - files
    - REST APIs
* ingest streaming data 
    - kafka
* small ETL 
    - route
    - entich 
    - validate
    - transform

## Requirements

* Java 1.8+
* Maven 3+
* Docker & Docker Compose

## Start the local environment 

``` 
git clone git@github.com:eSolutionsGrup/academy-nifi.git

cd ./academy-nifi/

docker-compose up -d
```


### Container shell access

```
$ docker exec -it ${docker_instance_name} bash
```

### MySQL CDC only for inventory database
```
$ echo 'binlog_do_db = inventory' >> /etc/mysql/conf.d/mysql.cnf
```
Note: exit from mysql docker instance and restart it 
```
$ docker-compose restart mysql 
```

### MySQL restore retail_db
```
$ docker exec -it ${mysql_docker_instance_name} bash

$ mysql -uroot -pdebezium
mysql> create database retail_db;
mysql> use retail_db;
mysql> source /content/retail_db.sql

```

## Extend NiFi (custom Processor and Controller Service)
```
$ mvn archetype:generate
```

Upload new nar files
```
$ docker cp nifi-custom-nar/target/nifi-custom-nar-1.0-SNAPSHOT.nar ${nifi_docker_instance_name}:/opt/nifi/nifi-current/lib/nifi-custom-nar-1.0-SNAPSHOT.nar
```
Note: 
starting with NiFi 1.9.0 allows live reloading for nars from $NIFI_HOME/extensions directory (in _config/nifi.properties_ there is now a _nifi.nar.library.autoload.directory=./extensions_)

Restart nifi docker instance
```
$ docker-compose restart nifi 
```
