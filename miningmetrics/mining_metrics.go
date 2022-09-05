package miningmetrics

import "time"

var StartMiningTime time.Time

func InitMiningTime() {
	StartMiningTime = time.Now()
}
func UpdateMiningTime(startTime time.Time) {
	StartMiningTime = startTime
}

func GetMiningTime() time.Time {
	return StartMiningTime
}
