package main

import (
	"math/rand"
	"strconv"
	"time"

	"gitlab.choicetechlab.com/common/log-client/logger"
)

func main() {
	logger.OnStartup()
	for range 100000 {
		tmpId := generateID()
		logger.Log_DEBUG(tmpId, "unset", "PUT", "START", "200", "REST request gained", nil)
		//logger.Log_DEBUG(tmpId, "unset", "PUT", "START", nil, "REST request gained", nil)
		// logger.Log_DEBUG(tmpId, "unset", "PUT", "START", "REST request received", nil)
		// tmpId = generateID()
		// logger.Log_DEBUG(tmpId, "expire", "DELETE", "START", "REST request processed", nil)
		// logger.Log_ERROR(tmpId, "expire", "DELETE", "START", "Error while processing the request", nil, nil)
	}
	time.Sleep(60 * time.Second)
}

func generateID() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}
