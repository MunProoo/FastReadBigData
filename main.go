package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {

	file, err := os.Open("LargeData/access.log")
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	err = fileRead(file)
	if err != nil {
		log.Print(err)
	}

	elapsed := time.Since(start)
	log.Printf("Binomial took %s", elapsed)

}

func fileRead(file *os.File) (err error) {
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 500*1024)
		return lines
	}}
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	slicePool := sync.Pool{New: func() interface{} {
		lines := make([]string, 100)
		return lines
	}}

	reader := bufio.NewReader(file)
	d := 0

	for {
		buf := linesPool.Get().([]byte)
		n, err := reader.Read(buf)
		// fmt.Printf("%v\n", n)
		buf = buf[:n]

		if n == 0 {
			if err != nil || err == io.EOF {
				log.Println(err)
				// log.Fatal(err)
				break
			}

			return err
		}

		nextUntilNewLine, err := reader.ReadBytes('\n')
		if err != io.EOF {
			buf = append(buf, nextUntilNewLine...)
		}

		d += processChunk(buf, &linesPool, &stringPool, &slicePool)
	}
	return nil
}

func processChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, slicePool *sync.Pool) int {

	//another wait group to process every chunk further
	var wg2 sync.WaitGroup
	// stringPool 불러오기
	logs := stringPool.Get().(string)

	// 청크데이터를 문자열 변환
	logs = string(chunk)
	linesPool.Put(chunk) // put back the chunk in pool

	// slicePool 가져오기
	logSlice := slicePool.Get().([]string)
	// 개행 기준으로 string 배열 생성
	logSlice = strings.Split(logs, "\n")

	// stringPool 반환
	stringPool.Put(logs)

	// 100줄만 읽기
	chunkSize := 100
	length := len(logSlice)

	// 청크 탐색
	for i := 0; i < length; i += chunkSize {
		wg2.Add(1)
		// 청크 계산
		start := i * chunkSize
		end := minInt((i+1)*chunkSize, len(logSlice))
		for i := start; i < end; i++ {
			text := logSlice[i]
			if len(text) == 0 {
				continue
			}
		}

		// 전처리 작업용
		/*
			go func(start, end int) {
				for i:=start; i<end; i++ {
					text := logSlice[i]
					if len(text) == 0 {
						continue
					}

				}
			}
		*/

	}
	// 청크 다 끝날떄까지 기다리기

	// slicePool 반환
	slicePool.Put(logSlice)
	return 1
}

func minInt(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
