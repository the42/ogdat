package ckan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/the42/ogdat/schedule"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Portal struct {
	*url.URL
}

func (p *Portal) GetAllMetaDataIDs() ([]string, error) {

	const alldatasets = "rest/dataset"
	var allsets []string

	alldataseturl, _ := url.Parse(alldatasets)
	jsonstream, err := getjson(alldataseturl.String(), false)
	if err != nil {
		return nil, err
	}

	bytedata, err := ioutil.ReadAll(jsonstream)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytedata, &allsets); err != nil {
		return nil, err
	}
	return allsets, nil
}

func (p *Portal) GetRevisionsetSince(t time.Time) ([]string, error) {

	revisions := fmt.Sprintf("rest/revision?since_time=%s", t)
	var revs []string

	revurl, _ := url.Parse(revisions)
	resp, err := getjson(p.ResolveReference(revurl).String(), false)
	if err != nil {
		return nil, err
	}

	bytedata, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bytedata, &revs); err != nil {
		return nil, err
	}

	return revs, nil
}

func stringslicetoiface(ss []string) []interface{} {
	slice := make([]interface{}, len(ss))
	for i, v := range ss {
		slice[i] = v
	}
	return slice
}

type concurrentSet struct {
	lock  sync.RWMutex
	value map[string]struct{}
}

func newSet() *concurrentSet {
	return &concurrentSet{value: make(map[string]struct{})}
}

func (cs *concurrentSet) add(key string) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	cs.value[key] = struct{}{}

}

func (cs *concurrentSet) deleteAll() {
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	cs.value = nil
}

type Revision struct {
	Packages []string `json:"packages"`
}

func (p *Portal) GetRevisionforID(id string) (*Revision, error) {
	revurl, _ := url.Parse("rest/revision/" + id)

	resp, err := getjson(p.ResolveReference(revurl).String(), false)
	if err != nil {
		return nil, err
	}

	bytedata, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, err
	}

	rev := &Revision{}
	if err := json.Unmarshal(bytedata, rev); err != nil {
		return nil, err
	}

	return rev, nil
}

func (p *Portal) GetChangedPackageIDsSince(t time.Time, workers int) ([]string, error) {
	revs, err := p.GetRevisionsetSince(t)
	if err != nil {
		return nil, err
	}

	scheduler := schedule.New(workers)
	conset := newSet()

	f := func(slice []interface{}) error {
		for _, val := range slice {
			revid, ok := val.(string)
			if !ok {
				panic("Interface value not of string type")
			}
			rev, err := p.GetRevisionforID(revid)
			if err != nil {
				conset.deleteAll()
				return err
			}
			for _, packageid := range rev.Packages {
				conset.add(packageid)
			}
		}
		return nil
	}

	<-scheduler.Schedule(f, stringslicetoiface(revs))

	changedids := make([]string, len(conset.value))
	idx := 0

	for key, _ := range conset.value {
		changedids[idx] = key
		idx++
	}
	return changedids, nil
}

func (p *Portal) GetDatasetStreamforID(id string, indent bool) (io.Reader, error) {

	const datasetid = "rest/dataset/"
	seturl, _ := url.Parse(datasetid + id)
	return getjson(p.ResolveReference(seturl).String(), indent)
}

func getjson(url string, indent bool) (io.Reader, error) {

	// number of retries to get data from the web
	const exhausted = 3

	var resp *http.Response
	var err error

	retry := 0
	for ; retry < exhausted; retry++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
	}
	if retry == exhausted {
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
