package scrapper

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	ccsv "github.com/tsak/concurrent-csv-writer"

	"github.com/PuerkitoBio/goquery"
)

type extractedJob struct {
	id       string
	title    string
	location string
	salary   string
	summary  string
}

// Scrape Indeed by a term
func Scrape(term string) {
	var baseURL string = "https://kr.indeed.com/jobs?q=" + term
	// var jobs []extractedJob
	c := make(chan []extractedJob)
	csvChannel := make(chan bool)
	file := writeJobsParallelReady()
	defer file.Close()

	totalPages := getPages(baseURL)

	for i := 0; i < totalPages; i++ {
		go getPage(baseURL, i, c)
	}
	for i := 0; i < totalPages; i++ {
		go writeJobsParallel(file, <-c, csvChannel)
	}
	// writeJobs(jobs)
	for i := 0; i < totalPages; i++ {
		<-csvChannel
	}
	fmt.Println("Extraction done!")
}

func getPage(url string, page int, mainChannel chan<- []extractedJob) {
	var jobs []extractedJob
	c := make(chan extractedJob)
	pageURL := url + "&start=" + strconv.Itoa(page*10)
	fmt.Println("Requesting", pageURL)
	res, err := http.Get(pageURL)
	checkErr(err)
	checkCode(res)
	defer res.Body.Close() // it prevents memory leaks

	doc, err := goquery.NewDocumentFromReader(res.Body) // res.Body is an I/O, so you need to close it (see above)
	checkErr(err)

	searchCards := doc.Find(".jobsearch-SerpJobCard")

	searchCards.Each(func(i int, card *goquery.Selection) {
		go extractJob(card, c)
	})
	for i := 0; i < searchCards.Length(); i++ {
		job := <-c
		jobs = append(jobs, job)
	}
	mainChannel <- jobs
}

func extractJob(card *goquery.Selection, c chan<- extractedJob) {
	id, _ := card.Attr("data-jk")
	title := CleanString(card.Find(".title>a").Text())
	location := CleanString(card.Find(".sjcl").Text())
	salary := CleanString(card.Find(".salaryText").Text())
	summary := CleanString(card.Find(".summary").Text())
	c <- extractedJob{
		id:       id,
		title:    title,
		location: location,
		salary:   salary,
		summary:  summary}
}

func CleanString(str string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(str)), " ")
}

func getPages(url string) int {
	pages := 0
	res, err := http.Get(url)
	checkErr(err)
	checkCode(res)

	defer res.Body.Close() // it prevents memory leaks

	doc, err := goquery.NewDocumentFromReader(res.Body) // res.Body is an I/O, so you need to close it (see above)
	checkErr(err)

	doc.Find(".pagination").Each(func(i int, s *goquery.Selection) {
		pages = s.Find("a").Length()
	})
	return pages
}

func writeJobs(jobs []extractedJob) {
	file, err := os.Create("jobs.csv")
	checkErr(err)

	// This solves encoding issues in Excel
	// utf8bom := []byte{0xEF, 0xBB, 0xBF}
	// file.Write(utf8bom)

	w := csv.NewWriter(file)
	defer w.Flush()

	headers := []string{"ID", "Title", "Location", "Salary", "Summary"}

	wErr := w.Write(headers)
	checkErr(wErr)

	for _, job := range jobs {
		jobSlice := []string{"https://kr.indeed.com/viewjob?jk=" + job.id, job.title, job.location, job.salary, job.summary}
		jwErr := w.Write(jobSlice)
		checkErr(jwErr)
	}
}

func writeJobsParallelReady() *ccsv.CsvWriter {
	file, err := ccsv.NewCsvWriter("jobs.csv")
	if err != nil {
		log.Fatalln("Cannot open jobs.csv for writing")
	}
	headers := []string{"ID", "Title", "Location", "Salary", "Summary"}
	wErr := file.Write(headers)
	checkErr(wErr)
	return file
}

func writeJobsParallel(file *ccsv.CsvWriter, jobs []extractedJob, c chan<- bool) {
	for _, job := range jobs {
		jobSlice := []string{"https://kr.indeed.com/viewjob?jk=" + job.id, job.title, job.location, job.salary, job.summary}
		jwErr := file.Write(jobSlice)
		checkErr(jwErr)
	}
	c <- true
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func checkCode(res *http.Response) {
	if res.StatusCode != 200 {
		log.Fatalln("Request failed with status:", res.StatusCode)
	}
}
