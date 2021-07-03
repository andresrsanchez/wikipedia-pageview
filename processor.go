package main

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CAPACITY = 25

func main() {
	valid, from, to := validateAndGet("yyyy-MM-ddTHH:00:00", "2020-01-01T01:00:00", "2020-01-01T07:00:00")
	if !valid {
		panic(fmt.Errorf("there is an error in dateFrom: %s or dateTo: %s", from, to))
	}

	dir, err := ioutil.TempDir("", "")
	check(err)

	blackList := getBlackList()

	ch := make(chan time.Time)
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(ch <-chan time.Time) {
			defer wg.Done()
			for d := range ch {
				path := download(dir, d)
				results := consumeFileAndReturnResults(path, blackList)
				writeResultsToFile(d.Format("20060102150000"), results)
				// wg.Done()
			}
		}(ch)
	}

	for d := from; !d.After(to); d = d.Add(1 * time.Hour) {
		ch <- d
	}

	close(ch)
	wg.Wait()

	err = os.RemoveAll(dir)
	check(err)
}

func download(dir string, date time.Time) string {
	url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%v/%v/pageviews-%s.gz",
		date.Year(), date.String()[0:7], date.Format("20060102-150000"))
	fmt.Println(url)
	resp, err := http.Get(url)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("there is an error getting the blacklist with statusCode: %v", resp.StatusCode)
	}
	check(err)
	defer resp.Body.Close()

	out, err := ioutil.TempFile(dir, "")
	check(err)
	fmt.Println(out.Name())

	defer out.Close()
	io.Copy(out, resp.Body)

	return out.Name()
}

func consumeFileAndReturnResults(path string, blackList map[string]map[string]bool) map[string]PriorityQueue {
	top := make(map[string]PriorityQueue)

	file, err := os.Open(path)
	check(err)

	gz, err := gzip.NewReader(file)
	check(err)

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

func writeResultsToFile(date string, results map[string]PriorityQueue) {
	err := os.MkdirAll("/dumps", os.ModePerm)
	check(err)

	var b strings.Builder
	for k, v := range results {
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

	err = ioutil.WriteFile(fmt.Sprintf("/dumps/%s", date), []byte(b.String()), 0644)
	check(err)
}

func getBlackList() map[string]map[string]bool {
	resp, err := http.Get("https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages")
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("there is an error getting the blacklist with statusCode: %v", resp.StatusCode)
	}
	check(err)
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		check(err)
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
			check(fmt.Errorf("there is and error with the splittedLine: %v", splittedLine))
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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func validateAndGet(format string, from string, to string) (bool, time.Time, time.Time) {
	parse := func(dateTime string) time.Time {
		if dateTime == "" {
			check(errors.New("cannot parse empty date"))
		}
		t2, e := time.Parse("2006-01-02T15:04:05", dateTime)
		check(e)
		return t2
	}
	fromDate := parse(from)
	toDate := parse(to)

	return fromDate.Before(toDate), fromDate, toDate
}
