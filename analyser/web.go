package main

import (
	restful "github.com/emicklei/go-restful"
	"github.com/garyburd/redigo/redis"
	"strconv"
)

func (an *analyser) GetRESTEntities() func(request *restful.Request, response *restful.Response) {
	return func(request *restful.Request, response *restful.Response) {
		id := request.PathParameter("id")
		sortorder := request.QueryParameter("sortorder")
		numds := request.QueryParameter("numds")

		var entity, command string
		nums := -1
		returnnums := false

		if sortorder == "asc" {
			command = "ZRANGE"
		} else {
			command = "ZREVRANGE"
		}

		if len(numds) > 0 {
			var err error
			returnnums, err = strconv.ParseBool(numds)
			if err != nil {
				returnnums = false
			}
		}

		var reply []interface{}
		var err error
		if returnnums {
			reply, err = redis.Values(an.rcon.Do(command, "entities", 0, -1, "WITHSCORES"))
		} else {
			reply, err = redis.Values(an.rcon.Do(command, "entities", 0, -1))
		}
		if err != nil {
			panic(err)
		}

		resultset := make([]UnitDSNums, 0)

		for len(reply) > 0 {
			a := UnitDSNums{}
			if returnnums {
				reply, err = redis.Scan(reply, &entity, &nums)
			} else {
				reply, err = redis.Scan(reply, &entity)

			}
			a.Numsets = nums
			a.Entity = entity
			if err != nil {
				panic(err)
			}
			resultset = append(resultset, a)
		}

		println("Hallo Welt", id)
		response.WriteEntity(resultset)
	}
}

func NewAnalyseOGDATRESTService(an *analyser) *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/entities/{id}").To(an.GetRESTEntities()).
		// for documentation
		Doc("Get entities within the Database").
		Param(ws.PathParameter("id", "the identifier of the entity to return. Leave empty for all")).
		Param(ws.QueryParameter("sortorder", "sort order of entities according to the number of assigned datasets. 'asc' for ascending, 'desc' for descending")).
		Param(ws.QueryParameter("numds", "if 'true', also return number of datasets")).
		Writes(struct{ Entities []UnitDSNums }{})) // to the response

	// 	ws.Route(ws.POST("/").To(saveApplication).
	// 		// for documentation
	// 		Doc("Create or update the Application node").
	// 		Reads(Application{})) // from the request
	return ws
}
