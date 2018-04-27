# Dashboard for online games
### Project idea

Build a real-time anomaly detection pipeline and a dashboard to show the aggregated results for users.

### What is the purpose, and most common use cases?

###### Purpose:

- real-time anomaly detection based on user reporting and simple stats
- dashboard that shows the most reported users
- shows the historical stats of any user

###### Use cases:

- detect game cheaters
- ~~detect social media trollers~~
- ~~offensive posts/twitts~~
- ~~fake news~~

### Which technologies are well-suited?

###### Datasets:

- PUBG match deaths and statistics
- Simulated reporting events
- Augment the datasets
- ~~Open Crawl - http://pubg.op.gg/*~~

###### Technologies:

- Kafka
- Flink
- Postgres ?
- Play

### What are the engineering challenges

- Joining two streams by different types and sizes of window

### Proposed architechture

Kafka -> Flink -> Postgres * -> Play

​	* Not sure about the database now. 

### What are the (quantitative) specifications/constriants

- events will come at 10K/s, 100K/s, and maybe 1M/s