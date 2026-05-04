package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	c "github.com/prathameshbordwekar/log-client/constants"
	dm "github.com/prathameshbordwekar/log-client/datamodels"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var (
	applicationName   string
	componentName     string
	kafkaLogTopic     string
	kafkaSASLUsername string
	kafkaSASLPassword string
	logLevelSet       string
	payloadLogging    bool
	producer          sarama.AsyncProducer
	logCh             chan *sarama.ProducerMessage
	kafkaBrokersList  []string
	once              sync.Once
)

func Log_DEBUG(transactionId string, interfaceName string, operationName string, transactionType string, statusCode interface{}, message string, payload interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered in Log_DEBUG: %v", r)
		}
	}()
	sendLogMessage(transactionId, interfaceName, operationName, transactionType, statusCode, message, c.LOG_LEVEL_DEBUG, payload, nil)
}

func Log_INFO(transactionId string, interfaceName string, operationName string, transactionType string, statusCode interface{}, message string, payload interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered in Log_INFO: %v", r)
		}
	}()
	sendLogMessage(transactionId, interfaceName, operationName, transactionType, statusCode, message, c.LOG_LEVEL_INFO, payload, nil)
}

func Log_WARN(transactionId string, interfaceName string, operationName string, transactionType string, statusCode interface{}, message string, payload interface{}) {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered in Log_WARN: %v", r)
		}
	}()
	sendLogMessage(transactionId, interfaceName, operationName, transactionType, statusCode, message, c.LOG_LEVEL_WARN, payload, nil)
}

func Log_ERROR(transactionId string, interfaceName string, operationName string, transactionType string, statusCode interface{}, message string, payload interface{}, stacktrace interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered in Log_ERROR: %v", r)
		}
	}()
	sendLogMessage(transactionId, interfaceName, operationName, transactionType, statusCode, message, c.LOG_LEVEL_ERROR, payload, stacktrace)
}

func sendLogMessage(transactionId string, interfaceName string, operationName string, transactionType string, statusCode interface{}, message string, logLevel string, payload interface{}, stacktrace interface{}) {
	if allowLogMessage(logLevel) {
		logMessage := &dm.LogMessage{
			ApplicationName: applicationName,
			ComponentName:   componentName,
			InterfaceName:   interfaceName,
			OperationName:   operationName,
			TransactionType: transactionType,
			Message:         message,
			LogLevel:        logLevel,
			Timestamp:       time.Now(),
		}

		if len(transactionId) > 0 {
			logMessage.TransactionId = transactionId
		} else {
			logMessage.TransactionId = uuid.NewString()
		}

		if sc, ok := statusCode.(string); ok {
			logMessage.StatusCode = sc
		}

		if payloadLogging && payload != nil {
			if pl, ok := payload.(string); ok {
				logMessage.Payload = pl
			} else {
				logMessage.Payload = fmt.Sprintf("%v", payload) // fallback
			}
			if len(logMessage.Payload) > c.MAX_LOG_SIZE {
				logMessage.Payload = logMessage.Payload[:c.MAX_LOG_SIZE] + "...[truncated]"
			}
		}

		if logLevel == c.LOG_LEVEL_ERROR && stacktrace != nil {
			if st, ok := stacktrace.(string); ok {
				logMessage.StackTrace = st
			} else {
				logMessage.StackTrace = fmt.Sprintf("%v", stacktrace) // fallback
			}
			if len(logMessage.StackTrace) > c.MAX_LOG_SIZE {
				logMessage.StackTrace = logMessage.StackTrace[:c.MAX_LOG_SIZE] + "...[truncated]"
			}
		}

		jsonLogMessage, marErr := json.Marshal(logMessage)
		if marErr != nil {
			panic(marErr)
		}

		// Create message
		msg := &sarama.ProducerMessage{
			Topic: kafkaLogTopic,
			Value: sarama.ByteEncoder(jsonLogMessage),
		}
		logCh <- msg
	}
}

func OnStartup() {
	once.Do(func() {
		err := godotenv.Load()
		if err != nil {
			log.Fatalf("Error loading .env file")
		}
		logCh = make(chan *sarama.ProducerMessage, 10000)
		applicationName = getProperty("APPLICATION_NAME")
		componentName = getProperty("COMPONENT_NAME")
		kafkaLogTopic = getProperty("KAFKA_LOG_TOPIC")
		kafkaSASLUsername = getProperty("KAFKA_SASL_USERNAME")
		kafkaSASLPassword = getProperty("KAFKA_SASL_PASSWORD")
		logLevelSet = getLogLevel()
		kafkaBrokersList = getKafkaBrokersList()
		payloadLogging = strings.ToLower(os.Getenv("PAYLOAD_LOGGING")) == "true"
		// Create new config
		config := sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		config.Producer.RequiredAcks = sarama.NoResponse
		config.Producer.Return.Successes = false
		config.Producer.Return.Errors = false
		config.Producer.Idempotent = false
		config.Producer.Retry.Max = 0
		config.Version = sarama.V2_6_0_0
		config.Net.SASL.Enable = true
		config.Net.SASL.User = kafkaSASLUsername
		config.Net.SASL.Password = kafkaSASLPassword
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.Handshake = true
		config.Net.TLS.Enable = false

		// Create sync producer
		producer, err = sarama.NewAsyncProducer(kafkaBrokersList, config)
		if err != nil {
			log.Fatalf("Failed to create KAFKA producer: %v", err)
		}
		startKafkaSender()
	})
}

func startKafkaSender() {
	go func() {
		for msg := range logCh {
			producer.Input() <- msg
		}
	}()
}

func getProperty(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("Environment variable %s is not set. Shutting down...\n", name)
	}
	return value
}

func getKafkaBrokersList() []string {
	value := os.Getenv("KAFKA_BROKERS_LIST")
	if value == "" {
		log.Fatalf("Environment variable KAFKA_BROKERS_LIST is not set. Shutting down...\n")
	}

	parts := strings.Split(value, ",")
	var list []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			list = append(list, trimmed)
		}
	}
	return list
}

func getLogLevel() string {
	value := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	if !isValidLogLevel(value) {
		log.Fatalf("Environment variable LOG_LEVEL should be set to one of these: %s, %s, %s or %s. Shutting down...\n", c.LOG_LEVEL_DEBUG, c.LOG_LEVEL_INFO, c.LOG_LEVEL_WARN, c.LOG_LEVEL_ERROR)
	}
	return value
}

func isValidLogLevel(level string) bool {
	switch level {
	case c.LOG_LEVEL_DEBUG, c.LOG_LEVEL_INFO, c.LOG_LEVEL_WARN, c.LOG_LEVEL_ERROR:
		return true
	default:
		return false
	}
}

func allowLogMessage(level string) bool {
	switch logLevelSet {
	case c.LOG_LEVEL_DEBUG:
		return level == c.LOG_LEVEL_DEBUG || level == c.LOG_LEVEL_INFO || level == c.LOG_LEVEL_WARN || level == c.LOG_LEVEL_ERROR
	case c.LOG_LEVEL_INFO:
		return level == c.LOG_LEVEL_INFO || level == c.LOG_LEVEL_WARN || level == c.LOG_LEVEL_ERROR
	case c.LOG_LEVEL_WARN:
		return level == c.LOG_LEVEL_WARN || level == c.LOG_LEVEL_ERROR
	case c.LOG_LEVEL_ERROR:
		return level == c.LOG_LEVEL_ERROR
	default:
		return false
	}
}
