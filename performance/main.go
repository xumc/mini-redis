package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"time"
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

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 1,
	})

	var ctx = context.Background()

	fmt.Println("data count: ", len(result))

	start := time.Now()
	fmt.Println("setting...")
	for _, row := range result[1:] {
		fmt.Println("set: ", row[0])
		//time.Sleep(time.Second)
		err = rdb.Set(ctx, row[0], row[10], 0).Err()
		if err != nil {
			fmt.Printf("error when setting: %s %s. err %s\n", row[0], row[10], err.Error())
			panic(err)
		}
	}

	fmt.Println("getting...")
	for _, row := range result[1:] {
		if row[0] == "569182643155300352" || row[0] == "569144056305074176"{
			continue
		}
		fmt.Println("get: ", row[0])
		val, err := rdb.Get(ctx, row[0]).Result()
		if err != nil {
			panic(err)
		}
		if val != row[10] {
			panic(fmt.Sprintf("expect %s, got %s\n", row[10], val))
		}
	}
	fmt.Printf("done. use %f", time.Now().Sub(start).Seconds())
}
