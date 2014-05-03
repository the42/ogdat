package main

import (
	"encoding/json"
	"fmt"
	restful "github.com/emicklei/go-restful"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"strconv"
	"time"
)

func (a *analyser) GetSortedSet(key string) func(request *restful.Request, response *restful.Response) {

	return func(request *restful.Request, response *restful.Response) {
		getentity := request.QueryParameter("id")
		sortorder := request.QueryParameter("sortorder")

		var entity string
		nums := -1

		var reply []interface{}
		var err error

		resultset := make([]IDNums, 0)

		rcon := a.pool.Get()
		defer rcon.Close()

		if len(getentity) > 0 {
			snums, err := redis.String(rcon.Do("ZSCORE", key, getentity))
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
			if len(snums) > 0 {
				if i, err := strconv.ParseInt(snums, 10, 0); err == nil {
					resultset = append(resultset, IDNums{ID: getentity, Numsets: int(i)})
				}
			}
		} else {
			if sortorder == "asc" {
				reply, err = redis.Values(rcon.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
			} else {
				reply, err = redis.Values(rcon.Do("ZREVRANGE", key, 0, -1, "WITHSCORES"))
			}
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}

			for len(reply) > 0 {
				reply, err = redis.Scan(reply, &entity, &nums)
				if err != nil {
					response.WriteError(http.StatusInternalServerError, err)
					return
				}
				resultset = append(resultset, IDNums{ID: entity, Numsets: nums})
			}
		}
		response.WriteEntity(resultset)
	}
}

func (a *analyser) GetTaxonomyDatasets(request *restful.Request, response *restful.Response) {

	taxonomy := request.PathParameter("which")
	subset := request.PathParameter("subset")

	var reply []interface{}
	var err error

	rcon := a.pool.Get()
	defer rcon.Close()

	type internalDataset struct {
		ID, CKANID  string
		Publisher   string
		Contact     string
		Description string
		Version     string
		Category    string
		GeoBBox     string
		GeoToponym  string
	}
	var internalsets []internalDataset
	reply, err = redis.Values(rcon.Do("SORT", datasetskey+":"+taxonomy+":"+subset,
		"BY", "nosort",
		"GET", datasetkey+":*->ID",
		"GET", datasetkey+":*->CKANID",
		"GET", datasetkey+":*->Publisher",
		"GET", datasetkey+":*->Contact",
		"GET", datasetkey+":*->Description",
		"GET", datasetkey+":*->Version",
		"GET", datasetkey+":*->Category",
		"GET", datasetkey+":*->GeoBBox",
		"GET", datasetkey+":*->GeoToponym"))
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if err = redis.ScanSlice(reply, &internalsets); err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	var responseset []Dataset
	for _, is := range internalsets {
		ds := Dataset{ID: is.ID,
			CKANID:      is.CKANID,
			Publisher:   is.Publisher,
			Contact:     is.Contact,
			Description: is.Description,
			Version:     is.Version,
			GeoBBox:     is.GeoBBox,
			GeoToponym:  is.GeoToponym}

		var strcats []string
		if len(is.Category) > 0 {
			if err := json.Unmarshal([]byte(is.Category), &strcats); err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
		}
		ds.Category = strcats
		responseset = append(responseset, ds)
	}
	response.WriteEntity(responseset)
}

func (a *analyser) GetAN003Data(request *restful.Request, response *restful.Response) {

	id := request.PathParameter("id")

	var reply []interface{}
	var err error

	rcon := a.pool.Get()
	defer rcon.Close()

	reply, err = redis.Values(rcon.Do("LRANGE", an003+":"+id, 0, -1))

	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	var responseset []URLCheckRecord
	var s string

	for len(reply) > 0 {
		if reply, err = redis.Scan(reply, &s); err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		var item URLCheckRecord
		if err := json.Unmarshal([]byte(s), &item); err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}

		responseset = append(responseset, item)
	}
	response.WriteEntity(responseset)
}

func (a *analyser) GetAN003TaxonomyData(taxonomy string) func(request *restful.Request, response *restful.Response) {

	return func(request *restful.Request, response *restful.Response) {

		subset := request.PathParameter("subset")
		var keyreply, datareply []interface{}
		var err error

		rcon := a.pool.Get()
		defer rcon.Close()

		keyreply, err = redis.Values(rcon.Do("ZRANGE", an003+":"+taxonomy+":"+subset, 0, -1))

		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}

		var key string
		var responseset []URLCheckRecord

		// TODO: Nested redis calls. Better to use MULTI here?
		for len(keyreply) > 0 {
			if keyreply, err = redis.Scan(keyreply, &key); err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
			datareply, err = redis.Values(rcon.Do("LRANGE", an003+":"+key, 0, -1))
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}

			var data string
			for len(datareply) > 0 {
				if datareply, err = redis.Scan(datareply, &data); err != nil {
					response.WriteError(http.StatusInternalServerError, err)
					return
				}

				var item URLCheckRecord
				if err = json.Unmarshal([]byte(data), &item); err != nil {
					response.WriteError(http.StatusInternalServerError, err)
					return
				}
				responseset = append(responseset, item)
			}
		}
		response.WriteEntity(responseset)
	}
}

func (a *analyser) GetDataset(request *restful.Request, response *restful.Response) {

	id := request.PathParameter("id")

	var reply []interface{}
	var err error

	rcon := a.pool.Get()
	defer rcon.Close()

	reply, err = redis.Values(rcon.Do("HMGET", datasetkey+":"+id,
		"ID",
		"CKANID",
		"Publisher",
		"Contact",
		"Description",
		"Version",
		"Category",
		"GeoBBox",
		"GeoToponym"))
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(reply) == 0 {
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Record not found"))
		return
	}

	var (
		ID, CKANID  string
		Publisher   string
		Contact     string
		Description string
		Version     string
		Category    string
		GeoBBox     string
		GeoToponym  string
	)

	if _, err = redis.Scan(reply,
		&ID,
		&CKANID,
		&Publisher,
		&Contact,
		&Description,
		&Version,
		&Category,
		&GeoBBox,
		&GeoToponym); err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ds := Dataset{ID: ID,
		CKANID:      CKANID,
		Publisher:   Publisher,
		Contact:     Contact,
		Description: Description,
		Version:     Version,
		GeoBBox:     GeoBBox,
		GeoToponym:  GeoToponym}

	if len(Category) > 0 {
		var strcats []string
		if err := json.Unmarshal([]byte(Category), &strcats); err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		ds.Category = strcats
	}

	response.WriteEntity(ds)
}

func (a *analyser) GetCheckResult(request *restful.Request, response *restful.Response) {

	id := request.PathParameter("id")

	var reply []interface{}
	var err error

	rcon := a.pool.Get()
	defer rcon.Close()

	reply, err = redis.Values(rcon.Do("HMGET", checkkey+":"+id, "CheckStatus", "CKANID", "Hittime"))
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(reply) == 0 {
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Record not found"))
		return
	}

	var (
		CKANID  string
		Hittime string
		Status  string
	)

	if _, err = redis.Scan(reply, &Status, &CKANID, &Hittime); err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	var checkStatus []CheckStatus
	checkrecord := CheckRecord{CKANID: CKANID}
	if len(Status) > 0 {

		if err := json.Unmarshal([]byte(Status), &checkStatus); err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		checkrecord.CheckStatus = checkStatus
	}
	if len(Hittime) > 0 {
		hittime, err := time.Parse(RedigoTimestamp, Hittime)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		checkrecord.Hittime = hittime
	}

	response.WriteEntity(checkrecord)
}

func NewAnalyseOGDATRESTService(an *analyser) *restful.WebService {
	ws := new(restful.WebService)
	ws.Path(apibasepath()).
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	ws.Filter(restful.OPTIONSFilter())

	cors := restful.CrossOriginResourceSharing{CookiesAllowed: false, Container: restful.DefaultContainer}
	ws.Filter(cors.Filter)

	ws.Route(ws.GET("/taxonomy/entities").To(an.GetSortedSet("taxonomy:entities")).
		Doc("Retouriert Open Data anbietende Verwaltungseinheiten und deren Anzahl an Datensätze").
		Operation("getentitiescount").
		Param(ws.QueryParameter("id", "Verwaltungseinheit, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/taxonomy/versions").To(an.GetSortedSet("taxonomy:versions")).
		Doc("Retourniert welche Version der Metadatenbeschreibung für OGD verwendet wird").
		Operation("getversioncount").
		Param(ws.QueryParameter("id", "Version der Metadatenbeschreibung, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Version der Metadatenbeschreibung nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/taxonomy/toponyms").To(an.GetSortedSet("taxonomy:toponyms")).
		Doc("Retourniert welche geographischen Abdeckungen in den OGD-Datensätzen spezifiziert sind").
		Operation("gettoponymscount").
		Param(ws.QueryParameter("id", "Geographische Abdeckung, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der geographischen Abdeckung nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/taxonomy/categories").To(an.GetSortedSet("taxonomy:categories")).
		Doc("Retourniert welche Kategorien in den OGD-Datensätzen spezifiziert sind").
		Operation("getcategoriescount").
		Param(ws.QueryParameter("id", "Kategorie, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Kategorien nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/datasets/taxonomy/{which}/{subset}").To(an.GetTaxonomyDatasets).
		Doc("Retourniert innerhalb der Taxonomie which die Datensätze nach subset").
		Operation("getdatasetsfortaxonomy").
		Param(ws.PathParameter("which", "Taxonomie nach der die Datensätze retourniert werden sollen")).
		Param(ws.PathParameter("subset", "Subset der Datensätze innerhalb der Taxonomie")).
		Writes(struct{ Datasets []Dataset }{}))

	ws.Route(ws.GET("/datasets/taxonomy/{which}").To(an.GetTaxonomyDatasets).
		Doc("Retourniert innerhalb der Taxonomie which jene Datensätze, die als Zeichenlänge 0 haben").
		Operation("getemptytaxonomydatasets").
		Param(ws.PathParameter("which", "Taxonomie nach der die Datensätze retourniert werden sollen")).
		Writes(struct{ Datasets []Dataset }{}))

	ws.Route(ws.GET("/dataset/{id}").To(an.GetDataset).
		Doc("Retourniert Metadateninformationen zum Datensatz mit id").
		Operation("getdatasetdetails").
		Param(ws.PathParameter("id", "Eindeutige Kennung des Datensatzes")).
		Writes(struct{ Datasets []Dataset }{}))

	ws.Route(ws.GET("/check/taxonomy/entities").To(an.GetSortedSet("check:entities")).
		Doc("Retourniert für die Verwaltungseinheiten die Anzahl der verfügbaren Checkergebnisse").
		Operation("getcheckentities").
		Param(ws.QueryParameter("id", "Verwaltungseinheit, für die Anzahl der Checkergebnisse retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/check/{id}").To(an.GetCheckResult).
		Doc("Retourniert Informationen des Checkergebnisses zum Datensatz mit id").
		Operation("getcheckdetails").
		Param(ws.PathParameter("id", "Eindeutige Kennung des Datensatzes")).
		Writes(struct{ CheckRecord []CheckRecord }{}))

	ws.Route(ws.GET("/analyse/" + an002 + "/entities").To(an.GetSortedSet(an002 + ":entities")).
		Doc("Welche Verwaltungseinheiten haben innerhalb eines Datensatzes identische Ressourcen?").
		Operation("getanalyse002entities").
		Param(ws.QueryParameter("id", "Verwaltungseinheit, für die Anzahl der identischen Ressourcen innerhalb eines Datensatzes retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/analyse/" + an003 + "/entities").To(an.GetSortedSet(an003 + ":entities")).
		Doc("Welche Verwaltungseinheiten haben Datensätze mit URLs, die nicht aufgelöst werden können?").
		Operation("getanalyse003entities").
		Param(ws.QueryParameter("id", "Verwaltungseinheit, für die Anzahl der nicht auflösbaren URLs retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/analyse/" + an003 + "/entities/{subset}").To(an.GetAN003TaxonomyData("entities")).
		Doc("Retourniere die ermittleten Fehler für jene Datensätze von Verwaltungseinheit {subset} wo Links nicht aufgelöst werden konnten").
		Operation("getanalyse003entitiesdata").
		Param(ws.PathParameter("subset", "Verwaltungseinheit, für welche die Daten retourniert werden sollen")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/analyse/" + an003 + "/{id}").To(an.GetAN003Data).
		Doc("Retourniert detailierte Informationen zum URL-Check für den Datensatz mit id").
		Operation("getanalyse003details").
		Param(ws.PathParameter("id", "Eindeutige Kennung des Datensatzes")).
		Writes(struct{ CheckRecord []URLCheckRecord }{}))

	// 	ws.Route(ws.POST("/").To(saveApplication).
	// 		// for documentation
	// 		Doc("Create or update the Application node").
	// 		Reads(Application{})) // from the request
	return ws
}
