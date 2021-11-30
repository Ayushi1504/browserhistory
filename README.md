# BROWSER HISTORY

Google chrome saves our surfing data in local sqlite3 db.
With this project i have tried to create an application which will live monitor the stream of my surfing history on google chrome.
To achieve this spark streaming is used. Since jdbc streaming isnt yet a functionality in spark. 
I have connected to the sql db and exported the required data saved it as a csv file .
Spark Stream later reads the csv file and provides live update to which urls have been visited the most.

This is a small utility i made for better Time Management.

## Points of improvement

The urls can be mined to get data to categorize them into broad categories like "Research","Entertainment","Financial","Social Media"
and for this spark Mllib library can be used.

Hence for any user , broad category of time spent on different urls will be visible then to better improve thei time.

As a proof of the work done till now , i have been working on this project hence google a lot about spark problems. Hence the below screenshot validates the same.
