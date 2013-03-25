package main

import (
	"encoding/json"
	"fmt"
	"github.com/the42/ogdat/ogdatv21"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
)

const ogdatdataseturl = "http://www.data.gv.at/katalog/api/2/rest/dataset/"

type Portal struct {
	*url.URL
}

func (p *Portal) GetAllMetaDataIDs() ([]ogdatv21.Identifier, error) {

	var allsets []ogdatv21.Identifier
	resp, err := http.Get(p.String())
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

func (p *Portal) GetMetadataforID(id ogdatv21.Identifier) (*ogdatv21.MetaData, error) {
	resp, err := http.Get(path.Join(p.URL.String(), id.String()))
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

func NewDataPortalAPIEndpoint(s string) *Portal {
	u, err := url.Parse(s)
	if err != nil {
		panic(fmt.Sprintf("MetaData API cannot be initialized: %s", err))
	}
	return &Portal{u}
}
