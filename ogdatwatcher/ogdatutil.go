package main

import (
	"encoding/json"
	"github.com/the42/ogdat/ogdatv21"
	"io/ioutil"
	"net/http"
)

const dataseturl = "http://www.data.gv.at/katalog/api/2/rest/dataset/"

func getalldatasetids() ([]ogdatv21.Identifier, error) {

	var allsets []ogdatv21.Identifier
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

func getmetadataforidentifier(id ogdatv21.Identifier) (*ogdatv21.MetaData, error) {
	resp, err := http.Get(dataseturl + id.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data := &ogdatv21.MetaData{}
	if err := json.Unmarshal(bytedata, data); err != nil {
		return nil, err
	}
	return data, nil
}
