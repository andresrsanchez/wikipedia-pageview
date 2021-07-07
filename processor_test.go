package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// func Test_main(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			main()
// 		})
// 	}
// }

var dir string

func TestMain(m *testing.M) {
	setup := func() string {
		dir, err := ioutil.TempDir("", "")
		check(err)
		return dir
	}

	dir = setup()
	code := m.Run()

	teardown := func() {
		err := os.RemoveAll(dir)
		check(err)
	}
	teardown()

	os.Exit(code)
}

func Test_download(t *testing.T) {
	type args struct {
		dir  string
		date time.Time
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should_works_fine",
			args: args{
				dir:  dir,
				date: time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC),
			},
			want: filepath.Join(dir, time.Date(2020, 01, 01, 12, 00, 00, 0, time.UTC).Format("20060102-150000")),
		},
		{
			name: "should_fail_because_wikifile_not_exists",
			args: args{
				dir:  dir,
				date: time.Date(2022, 01, 01, 12, 00, 00, 0, time.UTC),
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := download(tt.args.dir, tt.args.date); got != tt.want {
				t.Errorf("download() = %v, want %v", got, tt.want)
			}

		})
	}
}

func Test_writeResultsToFile(t *testing.T) {
	type args struct {
		date    string
		results map[string]PriorityQueue
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeResultsToFile(tt.args.date, tt.args.results)
		})
	}
}

func Test_getBlackList(t *testing.T) {
	tests := []struct {
		name string
		want map[string]map[string]bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getBlackList(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getBlackList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_check(t *testing.T) {
	type args struct {
		e error
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check(tt.args.e)
		})
	}
}

func Test_validateAndGet(t *testing.T) {
	type args struct {
		format string
		from   string
		to     string
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 time.Time
		want2 time.Time
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := validateAndGet(tt.args.format, tt.args.from, tt.args.to)
			if got != tt.want {
				t.Errorf("validateAndGet() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("validateAndGet() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("validateAndGet() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}
