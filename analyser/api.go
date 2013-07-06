package main

import (
	restful "github.com/emicklei/go-restful"
	"github.com/garyburd/redigo/redis"
	"strconv"
)

func (a *analyser) GetRESTEntities(request *restful.Request, response *restful.Response) {

	getentity := request.QueryParameter("entity")
	sortorder := request.QueryParameter("sortorder")

	var entity string
	nums := -1

	var reply []interface{}
	var err error

	resultset := make([]UnitDSNums, 0)
	
	rcon := a.pool.Get()
	defer rcon.Close()

	if len(getentity) > 0 {
		snums, err := redis.String(rcon.Do("ZSCORE", "entities", getentity))
		if err != nil {
			panic(err)
		}
		if len(snums) > 0 {
			if i, err := strconv.ParseInt(snums, 10, 0); err == nil {
				resultset = append(resultset, UnitDSNums{Entity: getentity, Numsets: int(i)})
			}
		}
	} else {
		if sortorder == "asc" {
			reply, err = redis.Values(rcon.Do("ZRANGE", "entities", 0, -1, "WITHSCORES"))
		} else {
			reply, err = redis.Values(rcon.Do("ZREVRANGE", "entities", 0, -1, "WITHSCORES"))
		}
		if err != nil {
			panic(err)
		}

		for len(reply) > 0 {
			reply, err = redis.Scan(reply, &entity, &nums)
			if err != nil {
				panic(err)
			}
			resultset = append(resultset, UnitDSNums{Entity: entity, Numsets: nums})
		}
	}

	response.WriteEntity(resultset)
}

func NewAnalyseOGDATRESTService(an *analyser) *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/entities").To(an.GetRESTEntities).
		// for documentation
		Doc("Liefert Verwaltungseinheit und deren Anzahl an Datensätze").
		Param(ws.QueryParameter("entity", "Verwaltungseinheit, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []UnitDSNums }{})) // to the response

	// 	ws.Route(ws.POST("/").To(saveApplication).
	// 		// for documentation
	// 		Doc("Create or update the Application node").
	// 		Reads(Application{})) // from the request
	return ws
}
