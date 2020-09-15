# Sparkify Data Lake - Spark ETL
Repository used for Data Lake with Spark project.

## Introduction 

This project contains an ETL pipeline that takes Sparkify data from S3 and processes it with Spark. 
Below you can find which tables are created as part of this ETL process. 
Tables are uploaded to S3 destination as parquet file, partitioned and into separate directories.

This is a learning project and not a real-world application. Use with caution. Sparkify does not exist.

## Tables and schemas 


Input data is taken from S3, transformed and loaded into tables relevant for analytics: 

1. songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
Relative path on S3: `analytics/songplays/`

![songplays table](https://imgur.com/oDLuT77.jpeg)

2. users (user_id, first_name, last_name, gender, level)
Relative path on S3: `analytics/users/`

![users table](https://imgur.com/J25Vp4D.jpeg)

3. songs (song_id, title, artist_id, year, duration)
Relative path on S3: `analytics/songs/`

![songs Table](https://imgur.com/kPVZtFU.jpeg)

4. artists (artist_id, name, location, latitude, longitude)
Relative path on S3: `analytics/artists/`

![artists Table](https://imgur.com/3qIawRW.jpeg)

5. time (start_time, hour, day, week, month, year, weekday)
Relative path on S3: `analytics/time/`

![time table](https://imgur.com/eqvRvzR.jpeg)

While tables 2-5 are used to organise data in dimensions (which users Sparkify has, which songs are in database, etc.), 
table 1. is a fact table designed for analytics. 


## Running the project 
### Pre-requisites 
1. Make sure you have AWS account and it's credentials at hand. 
2. Make sure you have Python 3 installed, it's required to run the scripts. 
3. If you want to run analytical queries too, please install pyarrow with command `pip install pyarrow` from the Terminal. 

### Process
1. **WARNING: Never commit AWS Credentials to Github or share them with others.**
   Put relevant AWS credentials into `dl.cfg`, as it's necessary for all next steps. 
2. Run `etl.py` to start an ETL process. This will only work once you did step 1. In scope of this step, data will be read from S3
3. Example of analytical queries can also be found in example notebook `queries.ipynb`. 

# License
Please refer to `LICENSE.md` to understand how to use this project for personal purposes.
