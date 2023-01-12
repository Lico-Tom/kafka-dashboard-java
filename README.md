# kafka-dashboard-java
## backend api command
### topic
#### create topic
```bash
curl -iv -X PUT -H "Content-Type: application/json" http://localhost:10013/api/kafka/topics -d '{"name":"test"}'
```
#### list topic
```bash
curl -iv -X GET -H "Content-Type: application/json" http://localhost:10013/api/kafka/topics
```
