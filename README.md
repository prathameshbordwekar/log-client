# log-client



## Getting started

add these properties to the .env file of GO application where this project is imported:

APPLICATION_NAME = "LMS"
COMPONENT_NAME = "expiry-cron"
KAFKA_BROKERS_LIST = "192.168.16.70:9095"
KAFKA_LOG_TOPIC = "uat.common.logs.create.v1"
KAFKA_SASL_USERNAME = ""
KAFKA_SASL_PASSWORD = ""
PAYLOAD_LOGGING = "true"
LOG_LEVEL = "DEBUG"
