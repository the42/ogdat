CREATE OR REPLACE FUNCTION insertorupdatemetadatainfo(IN inckanid character varying, IN inid character varying, IN pub character varying, IN cont character varying, IN descr text, IN invers character varying, IN incategory json, IN stime timestamp with time zone, IN ingeobbox character varying, IN ingeotoponym character varying, OUT datasetsysid integer, OUT isnew boolean)
  RETURNS record AS
$BODY$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dataset WHERE ckanid=inckanid LIMIT 1) THEN
    INSERT INTO dataset(ckanid, id, publisher, contact, description, vers, category, geobbox, geotoponym)
    VALUES (inckanid, inid, pub, cont, descr, invers, incategory, ingeobbox, ingeotoponym)
    RETURNING sysid INTO datasetsysid;

    -- Write status line about newly inserted metadata 
    INSERT INTO status(datasetid, hittime, status)
    VALUES (datasetsysid, stime, 'inserted');

    isnew := true;
  ELSE
    UPDATE dataset
    SET publisher=pub,
      contact=cont,
      description=descr,
      vers=invers,
      category=incategory,
      geobbox=ingeobbox,
      geotoponym=ingeotoponym
    WHERE ckanid=inckanid
    RETURNING sysid INTO datasetsysid;

    -- The status is append only to allow for time series analysis
    INSERT INTO status(datasetid, hittime, status)
    VALUES (datasetsysid, stime, 'updated'); -- however retain the information, that the row was updated

    isnew := false;
  END IF;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

