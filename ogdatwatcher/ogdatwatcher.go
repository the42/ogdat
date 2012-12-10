package main

import (
	"cgl.tideland.biz/net/atom"
	"encoding/json"
	"fmt"
	"github.com/the42/ogdat"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

const DEBUG = true
const dataseturl = "http://www.data.gv.at/katalog/api/2/rest/dataset/"

func getdataforid(id ogdat.Identfier) (*ogdat.MetaData, error) {
	data := &ogdat.MetaData{}

	resp, err := http.Get(dataseturl + id.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytedata, data); err != nil {
		return nil, err
	}
	return data, nil
}

func getalldatasetids() ([]ogdat.Identfier, error) {

	var allsets []ogdat.Identfier
	if !DEBUG {
		resp, err := http.Get(dataseturl)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		bytedata, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(bytedata, &allsets); err != nil {
			return nil, err
		}
	}
	return allsets, nil
}

func main() {
	allsets, err := getalldatasetids()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n\n", allsets)

	if len(allsets) > 0 {
		data, err := getdataforid(allsets[0])
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n\n", data)
	}

	url, err := url.Parse("http://www.data.gv.at/katalog/revision/list?format=atom")
	if err != nil {
		panic(err)
	}
	feed, err := atom.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v\n\n", feed)

	onedayago := time.Now().Add(-24 * time.Hour)
	fmt.Println(onedayago)
	atomtime, err := atom.ParseTime(feed.Updated)
	if err != nil {
		panic(err)
	}

	fmt.Println(atomtime)

	if atomtime.Before(onedayago) {
		fmt.Println("Datasets have not changed")
	} else {
		fmt.Println("Datasets have changed")
	}
}
