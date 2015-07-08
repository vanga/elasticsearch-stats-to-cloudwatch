# elasticsearch-stats-to-cloudwatch
Send elasticsearch node stats(from marvel) to AWS Cloudwatch

To Run on EC2 with an IAM role

Sends only Heap usage atm

Prerequisites boto,elasticsearch
```
pip install boto
pip install elasticsearch
```

Configure ES_HOST and region vars
