package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/the42/ogdat/database"
	"strings"
)

const (
	datasetskey = "datasets"
	datasetkey  = "dataset"

	checkkey = "check"

	taxonomyprefix = "taxonomy"

	catkey  = "categories"
	verskey = "versions"
	entkey  = "entities"
	topokey = "toponyms"
)

func (a analyser) populatedatasets() error {

	logger.Println("SQL: Retrieving datasets")
	sets, err := a.dbcon.GetDatasets()
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("Deleting base dataset info keys from Redis")

	rcon.Do("DEL", taxonomyprefix+":"+catkey, taxonomyprefix+":"+verskey, taxonomyprefix+":"+entkey, taxonomyprefix+":"+topokey)
	database.RedisConn{Conn:rcon}.DeleteKeyPattern(datasetskey+"*", datasetkey+"*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	logger.Println("Looping over datasets, populating information to Redis (this may take some time)")
	for _, set := range sets {

		// populate metadata version count
		if err = rcon.Send("ZINCRBY", taxonomyprefix+":"+verskey, 1, set.Version); err != nil {
			return err
		}
		// associate metadata version with ckanid
		if err = rcon.Send("SADD", datasetskey+":"+verskey+":"+set.Version, set.CKANID); err != nil {
			return err
		}

		// populate entity count
		if err = rcon.Send("ZINCRBY", taxonomyprefix+":"+entkey, 1, set.Publisher); err != nil {
			return err
		}
		// associate entity with ckanid
		if err = rcon.Send("SADD", datasetskey+":"+entkey+":"+set.Publisher, set.CKANID); err != nil {
			return err
		}

		// populate geographic toponym count
		if toponym := strings.TrimSpace(set.GeoToponym); len(toponym) > 0 {
			if err = rcon.Send("ZINCRBY", taxonomyprefix+":"+topokey, 1, toponym); err != nil {
				return err
			}
			// associate geographic toponym ckanid
			if err = rcon.Send("SADD", datasetskey+":"+topokey+":"+toponym, set.CKANID); err != nil {
				return err
			}

		}

		// populate category count
		for _, cat := range set.Category {
			if err = rcon.Send("ZINCRBY", taxonomyprefix+":"+catkey, 1, cat); err != nil {
				return err
			}
			// associate category with ckanid
			if err = rcon.Send("SADD", datasetskey+":"+catkey+":"+cat, set.CKANID); err != nil {
				return err
			}
		}

		// populate the dataset
		// use the flat, internal representation as .AddFlat will use Print(#v)
		// to fomat the slice of Category, which can not be meaningful parsed back
		is := internalDataset{ID: set.ID, CKANID: set.CKANID, Publisher: set.Publisher, Contact: set.Contact, Description: set.Description, Version: set.Version, GeoBBox: set.GeoBBox, GeoToponym: set.GeoToponym}

		rv, err := json.Marshal(set.Category)
		is.Category = string(rv)
		if err = rcon.Send("HMSET", redis.Args{}.Add("dataset:"+set.CKANID).AddFlat(&is)...); err != nil {
			return err
		}
	}
	logger.Println("Committing data to Redis")
	if _, err := rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatelastcheckresults() error {

	logger.Println("SQL: Retrieving last check results (this may take some time)")
	checkresults, err := a.dbcon.GetLastCheckResults()
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("Deleting check results info keys from Redis")

	database.RedisConn{Conn:rcon}.DeleteKeyPattern(checkkey + "*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	logger.Println("Looping over check results, populating information to Redis (this may take some time)")
	for _, checkresult := range checkresults {

		// populate the dataset
		record, err := json.Marshal(checkresult.CheckStatus)
		if err != nil {
			return err
		}
		if err = rcon.Send("HMSET", checkkey+":"+checkresult.CKANID, "CKANID", checkresult.CKANID, "Hittime", checkresult.Hittime, "Record", record); err != nil {
			return err
		}

		// associate entity with ckanid
		if err = rcon.Send("SADD", checkkey+":"+entkey+":"+checkresult.Publisher, checkresult.CKANID); err != nil {
			return err
		}
	}

	logger.Println("Committing data to Redis")
	if _, err = rcon.Do("EXEC"); err != nil {
		return err
	}

	return nil
}

func (a analyser) populatean001() error {
	const an001 = "an001"

	logger.Println("AN001: What publishers have multiple metadata sets, but within distinct sets point to the same data")

	logger.Println("AN001: SQL: Retrieving data")
	sets, err := a.dbcon.GetAN001Data()
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("AN001: Deleting keys from Redis")
	database.RedisConn{Conn:rcon}.DeleteKeyPattern(an001 + "*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {

		if err = rcon.Send("SADD", an001+":"+set.CKANID, set.Url); err != nil {
			return err
		}
		if err = rcon.Send("ZINCRBY", an001+":"+entkey+":"+set.Publisher, 1, set.CKANID); err != nil {
			return err
		}
	}
	logger.Println("AN001: Committing data to Redis")
	if _, err := rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatean002() error {
	const an002 = "an002"

	logger.Println("AN002: What publishers have multiple metadata sets, but within distinct sets point to the same data")

	logger.Println("AN002: SQL: Retrieving data")
	sets, err := a.dbcon.GetAN002Data()
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("AN002: Deleting keys from Redis")
	database.RedisConn{Conn:rcon}.DeleteKeyPattern(an002 + "*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {

		if err = rcon.Send("SADD", an002+":"+set.CKANID, set.Url); err != nil {
			return err
		}
		if err = rcon.Send("ZINCRBY", an002+":"+entkey+":"+set.Publisher, 1, set.CKANID); err != nil {
			return err
		}
	}
	logger.Println("AN002: Committing data to Redis")
	if _, err := rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatean003() error {
	const an003 = "an003"

	logger.Println("AN003: Which Links could not be checked and why? Also returns publisher information and check-time")
	logger.Println("AN003: SQL: Retrieving data")
	sets, err := a.dbcon.GetAN003Data()
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("AN003: Deleting keys from Redis")
	database.RedisConn{Conn:rcon}.DeleteKeyPattern(an003 + "*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {
		urlcheckerrors, _ := json.Marshal(set.Reason_Text)
		fieldkey := fmt.Sprintf("FieldID:%d", set.FieldID)
		if err = rcon.Send("HSET", an003+":"+set.CKANID, fieldkey, urlcheckerrors); err != nil {
			return err
		}
		if err = rcon.Send("ZINCRBY", an003+":"+entkey+":"+set.Publisher, len(set.Reason_Text), set.CKANID); err != nil {
			return err
		}
	}

	logger.Println("AN003: Committing data to Redis")
	if _, err := rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatebs001() error {
	// TODO: How many "last changed datasets" shall be retrieved?
	const num = 10

	const bs001 = "bs001"

	logger.Printf("BS001: Retrieve last %d changed datasets\n", num)

	logger.Println("BS001: SQL: Retrieving data")
	sets, err := a.dbcon.GetBS001Data(num)
	if err != nil {
		return err
	}

	rcon := a.pool.Get()
	defer rcon.Close()

	logger.Println("BS001: Deleting keys from Redis")
	database.RedisConn{Conn:rcon}.DeleteKeyPattern(bs001 + "*")

	if err := rcon.Send("MULTI"); err != nil {
		return nil
	}

	for _, set := range sets {
		if err = rcon.Send("SET", bs001+":"+set.CKANID, set.Time); err != nil {
			return err
		}
	}
	logger.Println("BS001: Committing data to Redis")
	if _, err := rcon.Do("EXEC"); err != nil {
		return err
	}
	return nil
}

func (a analyser) populatedatasetinfo() error {
	// BEGIN BASE INFO
	logger.Println("Starting populating datasets base info")
	if err := a.populatedatasets(); err != nil {
		return err
	}
	if err := a.populatelastcheckresults(); err != nil {
		return err
	}
	logger.Println("Done populating dataset base info")
	// END BASE INFO

	// BEGIN BASE ANALYSIS
	logger.Println("Starting dataset base analysis")
	if err := a.populatebs001(); err != nil {
		return err
	}
	logger.Println("Done dataset base analysis")
	// END BASE ANALYSIS

	// BEGIN DATASET ANALYSIS
	logger.Println("Starting dataset analysis")
	if err := a.populatean001(); err != nil {
		return err
	}
	if err := a.populatean002(); err != nil {
		return err
	}
	if err := a.populatean003(); err != nil {
		return err
	}
	logger.Println("Done dataset analysis")
	// END DATASET ANALYSIS
	return nil
}
