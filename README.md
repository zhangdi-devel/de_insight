# Dashboard for online games
### Project idea

Build a real-time anomaly detection pipeline and a dashboard to show the aggregated results for users.

### What is the purpose, and most common use cases?

###### Purpose:

- real-time anomaly detection based on user reporting and in-game aggregated stats
- dashboard that shows the most reported users

###### Use cases:

- detect game cheaters

### Which technologies are well-suited?

###### Datasets:

- PUBG match deaths and statistics
- Simulated reporting events

###### Technologies:

- Kafka
- Flink
- Postgres
- Dash

### What are the engineering challenges

- Stateful joining in Flink

### Proposed architechture

Kafka -> Flink -> Postgres -> Dash
