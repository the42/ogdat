package ckan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Portal struct {
	*url.URL
}

func (p *Portal) GetAllMetaDataIDs() ([]string, error) {

	const alldatasets = "rest/dataset"
	var allsets []string

	alldataseturl, _ := url.Parse(alldatasets)
	resp, err := http.Get(p.ResolveReference(alldataseturl).String())
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

func (p *Portal) GetJSONforID(id string, indent bool) (io.Reader, error) {

	const datasetid = "rest/dataset/"
	seturl, _ := url.Parse(datasetid + id)

	resp, err := http.Get(p.ResolveReference(seturl).String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bytedata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var jsondata io.Reader
	if indent {
		buf := new(bytes.Buffer)
		err = json.Indent(buf, bytedata, "", "  ")
		if err != nil {
			return nil, err
		}
		jsondata = buf
	} else {
		jsondata = bytes.NewBuffer(bytedata)
	}
	return jsondata, nil
}

func NewDataPortalAPIEndpoint(serverapi string, version string) *Portal {
	sapi, err := url.Parse(serverapi)
	if err != nil {
		panic(fmt.Sprintf("MetaData API cannot be initialized: %s", err))
	}
	sver, err := url.Parse(version)
	if err != nil {
		panic(fmt.Sprintf("MetaData API cannot be initialized: %s", err))
	}

	return &Portal{sapi.ResolveReference(sver)}
}
