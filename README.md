# Cloud Data Warehouse with Redshift
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-25273c?style=flat&logo=PostgreSQL)
![S3](https://img.shields.io/badge/-S3-25273c?style=flat&logo=amazon-aws)
![Redshift](https://img.shields.io/badge/-Redshift-25273c?style=flat&logo=amazon-aws)
![Boto3](https://img.shields.io/badge/-Boto3-25273c?style=flat&logo=amazon-aws)
![Python](https://img.shields.io/badge/-Python-25273c?style=flat&logo=python)

This main task of the project is to design a Cloud Data Warehouse for a fictional company called Sparkify, a music streaming platform like Spotify/Pandora. Sparkify has logged its data on user behaviour on its website. The log data, song data and JSON metadata about the  The data resides in an Amazon S3 Bucket.

The task is to design a Dimensional Data Model and ETL pipeline to build the Data Warehouse using Amazon Redshift as the data store so that the analysis team can coveniently wrangle the data.

***

## Datasets
### Logs Dataset
This dataset consists of logs on user behvaiour in JSON format. It is partitioned based on the year and month. The following is an example of the path to an example json file in the dataset. 

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

This data was generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the Song Dataset discussed in the next section. These simulate activity logs from a music streaming app based on specified configurations. 

### Songs Dataset
This is a subset of real data taken from the [Million Songs Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file in the dataset represents metadata about an individual song and its artist.The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.


```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

***

## Data Warehouse Schema
The following is the Star Schemas for the fact and dimension tables of the Data Warehouse

### Fact Table

songplays - records in log data associated with song plays
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
- user_id, first_name, last_name, gender, level

songs - songs in music database
- song_id, title, artist_id, year, duration

artists - artists in music database
- artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday
