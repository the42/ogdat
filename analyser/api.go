package main

import (
	restful "github.com/emicklei/go-restful"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"strconv"
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
			}

			for len(reply) > 0 {
				reply, err = redis.Scan(reply, &entity, &nums)
				if err != nil {
					response.WriteError(http.StatusInternalServerError, err)
				}
				resultset = append(resultset, IDNums{ID: entity, Numsets: nums})
			}
		}
		response.WriteEntity(resultset)
	}
}

func NewAnalyseOGDATRESTService(an *analyser) *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

		cors := restful.CrossOriginResourceSharing{ExposeHeaders:"X-My-Header", CookiesAllowed:false, Container:restful.DefaultContainer}
		ws.Filter(cors.Filter)

	ws.Route(ws.GET("/entities").To(an.GetSortedSet("entities")).
		Doc("Retouriert Open Data anbietende Verwaltungseinheiten und deren Anzahl an Datensätze").
		Param(ws.QueryParameter("id", "Verwaltungseinheit, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Verwaltungseinheiten nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/versions").To(an.GetSortedSet("versions")).
		Doc("Retourniert welche Version der Metadatenbeschreibung für OGD verwendet wird").
		Param(ws.QueryParameter("id", "Version der Metadatenbeschreibung, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Version der Metadatenbeschreibung nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/toponyms").To(an.GetSortedSet("toponyms")).
		Doc("Retourniert welche geographischen Abdeckungen in den OGD-Datensätzen spezifiziert sind").
		Param(ws.QueryParameter("id", "Geographische Abdeckung, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der geographischen Abdeckung nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	ws.Route(ws.GET("/categories").To(an.GetSortedSet("categories")).
		Doc("Retourniert welche Kategorien in den OGD-Datensätzen spezifiziert sind").
		Param(ws.QueryParameter("id", "Kategorie, für die Anzahl der Datensätze retourniert werden soll. Leer für alle")).
		Param(ws.QueryParameter("sortorder", "Sortierung der Kategorien nach Anzahl Datensätze. 'asc' für aufsteigend, 'desc' für absteigend (standard)")).
		Writes(struct{ Entities []IDNums }{}))

	// 	ws.Route(ws.POST("/").To(saveApplication).
	// 		// for documentation
	// 		Doc("Create or update the Application node").
	// 		Reads(Application{})) // from the request
	return ws
}
