package datamodels

import "time"

type LogMessage struct {
	TransactionId   string    `json:"transactionId"`
	ApplicationName string    `json:"applicationName"`
	ComponentName   string    `json:"componentName"`
	InterfaceName   string    `json:"interfaceName"`
	OperationName   string    `json:"operationName"`
	TransactionType string    `json:"transactionType"`
	StatusCode      string    `json:"statusCode"`
	Message         string    `json:"message"`
	LogLevel        string    `json:"loglevel"`
	Timestamp       time.Time `json:"timestamp"`
	Payload         string    `json:"payload"`
	StackTrace      string    `json:"stacktrace"`
}
