package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"sync"
	"time"
)

const (
	workerCount = 1000
)

func main() {
	f, err := os.Open("data.csv")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	reader := csv.NewReader(f)

	result, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	fmt.Println("data count: ", len(result))

	msgChan := make(chan []string, 1000)

	//////////////////////////Set///////////////////////////////////////
	start := time.Now()

	wg := sync.WaitGroup{}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			setWorker(msgChan)
		}()
	}

	for _, row := range result[1:] {
		msgChan <- row
	}

	wg.Wait()
	fmt.Printf("set cost: %f\v\n", time.Now().Sub(start).Seconds())

	/////////////////////////Get////////////////////////////////////////
	start = time.Now()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			getWorker(msgChan)
		}()
	}

	for _, row := range result[1:] {
		msgChan <- row
	}

	wg.Wait()
	fmt.Printf("get cost: %f\v\n", time.Now().Sub(start).Seconds())

	/////////////////////////Del////////////////////////////////////////
	start = time.Now()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			delWorker(msgChan)
		}()
	}

	for _, row := range result[1:] {
		msgChan <- row
	}

	wg.Wait()
	fmt.Printf("del cost: %f\v\n", time.Now().Sub(start).Seconds())
}

func setWorker(msgChan <- chan []string) {
	var ctx = context.Background()
	var rdb = createRedisClient()
	defer rdb.Close()

	for {
		select {
		case row := <- msgChan:
			err := rdb.Set(ctx, row[0], row[10], 0).Err()
			if err != nil {
				fmt.Printf("error when setting: %s %s. err %s\n", row[0], row[10], err.Error())
				panic(err)
			}
		default:
			return
		}
	}
}

func getWorker(msgChan chan []string) {
	var ctx = context.Background()
	var rdb = createRedisClient()
	defer rdb.Close()

	//rdb.TxPipelined(context.Background(), func(p redis.Pipeliner) error {
	//	return nil
	//})

	for {
		select {
		case row := <-msgChan:
			val, err := rdb.Get(ctx, row[0]).Result()
			if err != nil {
				fmt.Printf("error when getting: %s expect: %s. actual: %s, err %s\n", row[0], row[10], val, err.Error())
				panic(err)
			}
			if val != row[10] {
				fmt.Printf("error when getting: %s expect: %s. actual: %s\n", row[0], row[10], val)
				panic(err)
			}
		default:
			return
		}
	}
}

func delWorker(msgChan chan []string) {
	var ctx = context.Background()
	var rdb = createRedisClient()
	defer rdb.Close()

	for {
		select {
		case row := <-msgChan:
			err := rdb.Del(ctx, row[0]).Err()
			if err != nil {
				fmt.Printf("error when deleting: %s expect: %s. err %s\n", row[0], row[10], err.Error())
				panic(err)
			}
		default:
			return
		}
	}
}

func createRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "192.168.3.18:6379",
		PoolSize: 1,
	})
}
