
CREATE OR REPLACE FUNCTION markdatasetdeleted(IN inckanid character varying, IN stime timestamp with time zone)
  RETURNS status.sysid%TYPE AS
$BODY$
DECLARE
  retid status.sysid%TYPE;
BEGIN
  INSERT INTO status(datasetid, hittime, status)
  SELECT sysid, stime, 'deleted'
    FROM dataset
    WHERE ckanid = inckanid
  RETURNING sysid INTO retid;
  RETURN retid;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
