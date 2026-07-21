package main

import (
	"strconv"
	"time"
)

func itoa(i int) string { return strconv.Itoa(i) }

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
