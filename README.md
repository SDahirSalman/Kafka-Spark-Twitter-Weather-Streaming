# Kafka-Spark-Twitter-Weather-Streaming
A streaming app using tweepy, weatherapi and kafka that explores the relationship between weather conditions and tweets sentiments 

The program streams tweets and then extracts the tweet user's location and the sentiment of the tweet. That way we can explore whether the tweets of people at locations with favourable temperatures tend to be happier (positive sentiments) or sadder (negative sentiments). 

Using the tweepy library, tweets are scraped and fed into a kafka producer, all done within the tweet_stream_producer.py file. The temperature of the cities the twweets come from is also fetched using the openweatherapi. 

Within the kafka_spark_consumer_file, spark structured stream is employed as the kafka consumer which then feeds the dataframe into spark ml model and finally writes the data into cassandra. 


