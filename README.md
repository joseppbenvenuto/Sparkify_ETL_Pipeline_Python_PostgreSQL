# Sparkify ETL Pipeline PostgreSQL

## Project Description

Skarkify is a startup that wants to analyze song and user activity data that they've been collecting on their streaming app. With this initiative in mind a PostgreSQL database was developed in addition to an ETL pipline in Python. 

## Schema - Star

### Files & Description:

**test.ipynb -**  Using Python's SQL extension, script is developed to allow for database testing by viewing created tables.<br>
**create_tables.py -**  Python script is developed to drop and create tables allowing for felxability during the ETL pipelin development phase. <br>
**etl.ipynb -** A Jupyter notebook containing the first phase of development for Sparkify's ETL pipline.<br><br>
**etl.py -** Final ETL pipline script that extracts data from JSON files, transforms the data appropriately to allow for future analysis, and loads the data into Sparkify's database via fact and dimnesion tables.<br>
**sql_queries.py -** Contains all SQL queries used in the ETL pipline.<br><br>

### Tables

**Fact Table:**<br>
**songplays -** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables<br><br><br>

**Dimension Tables:**<br>
**users -** user_id, first_name, last_name, gender, level<br><br>
**songs -** song_id, title, artist_id, year, duration<br><br>
**artists -** artist_id, name, location, latitude, longitude<br><br>
**time -** start_time, hour, day, week, month, year, weekday<br><br>

