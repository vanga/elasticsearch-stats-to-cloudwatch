# elasticsearch-stats-to-cloudwatch
Python script to send elasticsearch node stats(from marvel) to AWS Cloudwatch

To run on EC2 with an IAM role

Sends only Heap usage atm

Prerequisites boto,elasticsearch
```
pip install boto
pip install elasticsearch
```

Configure ES_HOST and region vars

All ES nodes must be time synched (Becasue of the ES query which gets average data of last 2 minutes)

#### Tested on
Python 2.7.6
boto 2.38.0
