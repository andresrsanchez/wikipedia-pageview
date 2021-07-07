package main

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CAPACITY = 25

func main() {
	valid, from, to := validateAndGet("yyyy-MM-ddTHH:00:00", "2020-01-01T01:00:00", "2020-01-01T02:00:00")
	if !valid {
		panic(fmt.Errorf("there is an error in dateFrom: %s or dateTo: %s", from, to))
	}

	fmt.Println(os.TempDir())

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll("/dumps", os.ModePerm)
	if err != nil {
		panic(err)
	}

	blackList := getBlackList()

	ch := make(chan time.Time)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(ch <-chan time.Time) {
			defer wg.Done()
			for d := range ch {
				path := download(dir, d)
				results := consumeFileAndReturnResults(path, blackList)
				writeResultsToFile(d.Format("20060102150000"), results)
			}
		}(ch)
	}

	for d := from; !d.After(to); d = d.Add(1 * time.Hour) {
		ch <- d
	}

	close(ch)
	wg.Wait()

	err = os.RemoveAll(dir)
	if err != nil {
		panic(err)
	}
}

func download(dir string, date time.Time) string {
	url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%v/%v/pageviews-%s.gz",
		date.Year(), date.String()[0:7], date.Format("20060102-150000"))
	fmt.Println(url)
	resp, err := http.Get(url)
	if err == nil && resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		log.Printf("there is an error getting the wikimedia file with statusCode: %d and error: %s\n", resp.StatusCode, bodyString)
		return ""
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath.Join(dir, date.Format("20060102-150000")))
	if check(err) != nil {
		return ""
	}
	fmt.Println(out.Name())

	defer out.Close()
	io.Copy(out, resp.Body)

	return out.Name()
}

func consumeFileAndReturnResults(path string, blackList map[string]map[string]bool) map[string]PriorityQueue {
	top := make(map[string]PriorityQueue)

	file, err := os.Open(path)
	if check(err) != nil {
		return nil
	}

	gz, err := gzip.NewReader(file)
	if check(err) != nil {
		return nil
	}
	defer file.Close()
	defer gz.Close()

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		domain := line[0]
		title := line[1]

		if _, ok := blackList[domain][title]; ok {
			continue
		}
		views, _ := strconv.Atoi(line[2])

		if sortedValues, ok := top[domain]; ok {
			if sortedValues.Len() < CAPACITY {
				heap.Push(&sortedValues, &Item{
					priority: views,
					value:    title,
				})
				top[domain] = sortedValues
			} else if val, ok := sortedValues.Peek(); ok == nil && views > val.priority {
				heap.Pop(&sortedValues)
				heap.Push(&sortedValues, &Item{
					priority: views,
					value:    title,
				})
				top[domain] = sortedValues
			}
		} else {
			sortedValues = make(PriorityQueue, 0, CAPACITY)
			heap.Push(&sortedValues, &Item{
				priority: views,
				value:    title,
			})
			top[domain] = sortedValues
		}
	}
	return top
}

func writeResultsToFile(date string, results map[string]PriorityQueue) string {
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		v := results[k]
		stack := make(Stack, 0, v.Len())
		for v.Len() > 0 {
			element := heap.Pop(&v).(*Item)
			stack = stack.Push(fmt.Sprintf("%s %s %v", k, element.value, element.priority))
		}
		for len(stack) > 0 {
			var result string
			stack, result = stack.Pop()
			fmt.Fprintf(&b, "%s \n", result)
		}
	}

	fileName := fmt.Sprintf("/dumps/%s", date)
	err := ioutil.WriteFile(fileName, []byte(b.String()), 0644)
	if check(err) != nil {
		return ""
	}

	return fileName
}

func getBlackList() map[string]map[string]bool {
	resp, err := http.Get("https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages")
	if err != nil {
		panic(err)
	} else if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		log.Printf("there is an error getting the blacklist with statusCode: %d and error: %s\n", resp.StatusCode, bodyString)
		return nil
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	bodyString := string(bodyBytes)
	lines := strings.Split(bodyString, "\n")

	blackList := make(map[string]map[string]bool)
	for _, element := range lines {
		if element == "" {
			continue
		}
		splittedLine := strings.Split(element, " ")
		if len(splittedLine) != 2 {
			panic(fmt.Errorf("there is and error with the splittedLine: %v", splittedLine))
		}

		if titles := blackList[splittedLine[0]]; len(titles) == 0 {
			blackList[splittedLine[0]] = map[string]bool{splittedLine[1]: true}
		} else {
			containsTitle := titles[splittedLine[1]]
			if !containsTitle {
				titles[splittedLine[1]] = true
			}
		}
	}
	return blackList
}

func check(e error) error {
	if e != nil {
		log.Printf("%s\n", e.Error())
	}
	return e
}

func validateAndGet(format string, from string, to string) (bool, time.Time, time.Time) {
	parse := func(dateTime string) time.Time {
		if dateTime == "" {
			panic(errors.New("cannot parse empty date"))
		}
		t2, e := time.Parse("2006-01-02T15:04:05", dateTime)
		if e != nil {
			panic(e)
		}
		return t2
	}
	fromDate := parse(from)
	toDate := parse(to)

	return fromDate.Before(toDate), fromDate, toDate
}
