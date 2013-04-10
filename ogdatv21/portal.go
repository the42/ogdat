package ogdatv21

import (
	"encoding/json"
	"io"
	"io/ioutil"
)

func MetadatafromJSONStream(jsondata io.Reader) (*MetaData, error) {
	bytedata, err := ioutil.ReadAll(jsondata)
	if err != nil {
		return nil, err
	}

	data := &MetaData{}
	if err := json.Unmarshal(bytedata, data); err != nil {
		return nil, err
	}
	return data, nil
}
