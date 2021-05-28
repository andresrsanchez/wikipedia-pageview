package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

func main() {
	valid, from, to := validateAndGet("yyyy-MM-ddTHH:00:00", "2020-01-01T00:00:00", "2020-01-02T00:00:00")
	if !valid {
		panic(fmt.Errorf("there is an error in dateFrom: %s or dateTo: %s", from, to))
	}

	blackList := getBlackList()

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
		titles := blackList[splittedLine[0]]
		if len(titles) == 0 {
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

// func ProcessAndGetResultsFilePath(dateFrom string, dateTo string) {

// }

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
