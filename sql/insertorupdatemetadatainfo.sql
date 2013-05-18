
CREATE FUNCTION insertorupdatemetadatainfo(inckanid character varying, inid character varying, pub character varying, cont character varying, descr text, invers character varying, incategory json, stime timestamp with time zone, OUT datasetsysid integer, OUT isnew boolean) RETURNS record
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dataset WHERE ckanid=inckanid LIMIT 1) THEN
    INSERT INTO dataset(ckanid, id, publisher, contact, description, vers, category)
    VALUES (inckanid, inid, pub, cont, descr, invers, incategory)
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
      category=incategory
    WHERE ckanid=inckanid
    RETURNING sysid INTO datasetsysid;

    -- The status is insert only; possibly revise at a later time
    INSERT INTO status(datasetid, hittime, status)
    VALUES (datasetsysid, stime, 'updated'); -- however retain the information, that the row was updated

    isnew := false;
  END IF;
END;
$$;
