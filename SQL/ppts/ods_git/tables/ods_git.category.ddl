CREATE TABLE ods_git.category (
    catid smallint NOT NULL ENCODE raw
    distkey
,
        catgroup character varying(10) ENCODE lzo,
        catname character varying(10) ENCODE lzo,
        catdesc character varying(50) ENCODE lzo,
        catrank character varying(50) ENCODE lzo,
        catspend character varying(50) ENCODE lzo
) DISTSTYLE KEY
SORTKEY
    (catid);