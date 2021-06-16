package executor

import (
	"encoding/json"
	"fmt"

	"github.com/DataWorkbench/common/constants"
)

func GetEngine(options string) (respstring string, err error) {
	var resp constants.EngineResponseOptions

	resp.EngineType = "Flink"
	resp.EngineHost = "127.0.0.1"
	//resp.EngineHost = "flinkjobmanager"
	resp.EnginePort = "8081"

	respbyte, _ := json.Marshal(resp)
	respstring = string(respbyte)

	return
}

func FreeEngine(jobID string) {
	fmt.Println("free engine")
}
