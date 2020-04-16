# Queue-service
General Queue Service with multiple Topics having multiple Partitions

Design a Queue Service, allowing multiple subscribers on a topic. 
Each consumer can have multiple pollers – implement parallel polling using sub queues, assuming that producer can have ordering key used to send messages to the same sub queue.
— Code interfaces : (subscribe, publish, consume), allow extension for multiple pollers (initially taking just one poller).

Ensure what is expected from queue: Msg ordering, consistency, availability.
