package main

import (
	"encoding/json"
	"fmt"
	"github.com/the42/ogdat"
	"io/ioutil"
	"net/http"
)

const dataseturl = "http://www.data.gv.at/katalog/api/2/rest/dataset/"

func getdataforid(id ogdat.Identfier) (*ogdat.Core, error) {
	data := &ogdat.Core{}

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

func getdatasetids() ([]ogdat.Identfier, error) {
	var allsets []ogdat.Identfier

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
	return allsets, nil
}

func main() {
	allsets, err := getdatasetids()
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
}
