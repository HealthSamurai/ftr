-- Concept table
-- https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.1+Concept+File+Specification
DROP TABLE IF EXISTS concept;
--;--
DROP TABLE IF EXISTS tmp_concept;
--;--
CREATE TABLE concept
(
    id TEXT PRIMARY KEY,
    effectiveTime TEXT,
    active CHAR(1),
    moduleId TEXT,
    definitionStatusId TEXT
);
--;--
CREATE  TABLE tmp_concept
(LIKE concept INCLUDING DEFAULTS);
--;--
-- Description table
-- https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.2+Description+File+Specification
DROP TABLE IF EXISTS description;
--;--
DROP TABLE IF EXISTS tmp_description;
--;--
CREATE TABLE description
(
    id TEXT PRIMARY KEY,
    effectiveTime TEXT,
    active CHAR(1),
    moduleId TEXT,
    conceptId TEXT,
    languageCode TEXT,
    typeId TEXT,
    term TEXT,
    caseSignificanceId TEXT
);
--;--
CREATE  TABLE tmp_description
(LIKE description INCLUDING DEFAULTS);
--;--
-- Textual definition of concepts
DROP TABLE IF EXISTS textdefinition;
--;--
DROP TABLE IF EXISTS tmp_textdefinition;
--;--
CREATE TABLE textdefinition
(
    id TEXT PRIMARY KEY,
    effectiveTime TEXT,
    active CHAR(1),
    moduleId TEXT,
    conceptId TEXT,
    languageCode TEXT,
    typeId TEXT,
    term TEXT,
    caseSignificanceId TEXT
);
--;--
CREATE  TABLE tmp_textdefinition
(LIKE textdefinition INCLUDING DEFAULTS);
--;--
-- Relationship table
-- https://confluence.ihtsdotools.org/display/DOCRELFMT/4.2.3+Relationship+File+Specification
DROP TABLE IF EXISTS relationship;
--;--
DROP TABLE IF EXISTS tmp_relationship;
--;--
CREATE TABLE relationship
(
    id TEXT PRIMARY KEY,
    effectiveTime TEXT,
    active CHAR(1),
    moduleId TEXT,
    sourceId TEXT,
    destinationId TEXT,
    relationshipGroup TEXT,
    typeId TEXT,
    characteristicTypeId TEXT,
    modifierId TEXT
);
--;--
CREATE  TABLE tmp_relationship
(LIKE relationship INCLUDING DEFAULTS);
--;--
-- SDL table
-- http://people.apache.org/~dongsheng/horak/100309_dag_structures_sql.pdf
DROP TABLE IF EXISTS sdl;
--;--
DROP TABLE IF EXISTS tmp_sdl;
--;--
CREATE TABLE sdl
(
    src  text,
    dst  text,
    dist integer
);
--;--
CREATE  TABLE tmp_sdl
(LIKE sdl INCLUDING DEFAULTS);
--;--
create or replace function jsonb_object_nullif(
    _data jsonb
)
returns jsonb
as $$
    select nullif(jsonb_strip_nulls(_data)::text, '{}')::json
$$ language sql;
