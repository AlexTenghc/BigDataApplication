# Intro
• As an avid NBA enthusiast, I've always been fascinated by the incredible journey and statistics of basketball players. Inspired by my passion for the game, I've developed this application, the NBA Player Stats Tracker. This app serves as a centralized hub for fetching comprehensive data on individual NBA players, catering to the curiosity of fellow basketball aficionados like myself.
• The primary aim of this app is to provide seamless access to detailed statistics and insights into the careers of NBA athletes. From points scored, rebounds grabbed, assists made, to diverse metrics encompassing player height, weight, team affiliations, and more, this platform seeks to offer an immersive experience for exploring and analyzing player performance over their career spans.
# How to use
• In the main page, users can type in an NBA player's name and the stats of that player at every season would show up in rows.
• In the submit-stats page, users can type in all stats of a player in a specific season and it will be uploaded to the HBase.
# Step 1 - Create the Hive table
• beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver
• Log into Hive and create a table containing the data.
# Step 2 - Create the HBase table
• HBase shell
• Log into HBase and create the HBase table: create "hsiangct_final_NBA_stats", "cf"
# Step 3 - Link HBase table with Hive
# Step 4 - Start the application (You should start from here)
• On your terminal, log into aws server using ec2-user: ssh -i ~/.ssh/hsiangct_mpcs53014.pem ec2-user@ec2-3-143-113-170.us-east-2.compute.amazonaws.com
• Then: cd hsiangct/final/. Then run the app using: node app.js 3074 ec2-3-131-137-149.us-east-2.compute.amazonaws.com 8070 b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092
• Open the webpage using: ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3074. In this webpage, you can type in a NBA player's name and the app will return the statistics of that player in a yearly manner. Be sure to type in the correct name.
# Step 5 - Submit-html
• Open the webpage using: ec2-3-143-113-170.us-east-2.compute.amazonaws.com:3074/submit-stat.html. In this webpage, you can type in all data in a specific player and after clicking submit, the data would be added to HBase throught Kafka.
# Spped Layer
• Create a kafka topic called hsiangct_final_NBA
• SSH into hadoop using :ssh -i ~/.ssh/hsiangct_mpcs53014.pem hadoop@ec2-3-131-137-149.us-east-2.compute.amazonaws.com.
• Then: cd hsiangct/target.
• Run the kafka using: spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamNBAStats uber-SpeedLayer-1.0-SNAPSHOT.jar b-2.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-1.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092,b-3.mpcs53014kafka.o5ok5i.c4.kafka.us-east-2.amazonaws.com:9092

# Avoid data being processced twice
In order to avoid data being processed twice, I tried to add a timestamp in each data submitted in the speed layer. Also, I tried to add an endpoint which can be triggered to update batch layer. The timestamp uploaded in the batch layer trigger message would process data up to that timestamp. The mechanism can make sure that every data arrived after the timestamp would only be processed by speed layer. However I failed to figure out the code that should be in the scala file. This method is a way that I think can be applied to avoid data being processed twice.