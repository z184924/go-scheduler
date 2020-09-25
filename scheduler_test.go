package scheduler

import (
	"log"
	"testing"
	"time"
)

type myTask struct {
}

func (task *myTask) Run(parameter map[string]interface{}) bool {
	log.Print(parameter["print"])
	time.Sleep(1 * time.Second)
	return true
}
func TestCron_AddTask(t *testing.T) {
	scheduler := GetInstance()
	parameter := make(map[string]interface{})
	parameter["print"] = "aaaa"
	scheduler.AddTask(&myTask{}, "aaaa", parameter, "*/3 * * * * ?")
	parameter = make(map[string]interface{})
	parameter["print"] = "bbbb"
	scheduler.AddTask(&myTask{}, "bbbb", parameter, "*/2 * * * * ?")
	parameter = make(map[string]interface{})
	parameter["print"] = "cccc"
	scheduler.AddTask(&myTask{}, "cccc", parameter, "*/2 * * * * ?")
	parameter = make(map[string]interface{})
	parameter["print"] = "dddd"
	scheduler.AddTask(&myTask{}, "dddd", parameter, "*/3 * * * * ?")
	i := 0
	for {
		i++
		time.Sleep(10 * time.Second)
		log.Print("-----------")
		task := scheduler.GetTask("aaaa")
		log.Printf("RunCount:%d\n", task.RunCount)
		log.Printf("SuccessCount:%d\n", task.SuccessCount)
		log.Printf("AverageTime:%d\n", task.AverageTime)
		log.Printf("LongestTime:%d\n", task.LongestTime)
		log.Print("-----------")
		if task.RunFlag {
			scheduler.StopTask("aaaa")
		} else {
			scheduler.StartTask("aaaa")
		}
		if i == 5 {
			log.Print("delete bbbb")
			scheduler.RemoveTask("bbbb")
		}
		if i > 10 {
			return
		}
	}
}
