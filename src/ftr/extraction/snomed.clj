(ns ftr.extraction.snomed
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare]
            [ftr.utils.unifn.core :as u]
            [ftr.utils.core])
  (:import [java.sql
            PreparedStatement]))


(defn snomed-files
  "
  `sm-path` - Path to unzipped SNOMED CT bundle

  Description:
  List of SNOMED CT terminology snapshot file paths and resource names.

  file paths are constructed from `sm-path`.
  I.e. if `sm-path` is absolute, then result will have absolute paths

  resource names are second parts of underscore delimited file names.
  E.g. for sct2_Concept_... it will be Concept

  Return format: ({:name resource-name :path file-path} ...)"
  [sm-path]

  (mapcat
    (fn [sm-path']
      (let [snapshot-relative-path "Snapshot/Terminology"
            snapshot-path          (str sm-path' \/ snapshot-relative-path)]
        (->> (io/file snapshot-path)
             .list
             (filter #(str/ends-with? % ".txt"))
             (map
               (fn [filename]
                 (let [full-path     (str snapshot-path \/ filename)
                       ;; filenames in SNOMED CT are like
                       ;; sct2_Concept_Snapshot_...
                       resource-name (second (str/split filename #"_"))]
                   {:path full-path
                    :name resource-name}))))))
    (if (coll? sm-path)
      sm-path
      (list sm-path))))


(defn init-db-tables
  "Run sqlite database migration.
  Queries are splitted with string: --;-- "
  [db]
  (let [migration-sql (slurp (io/resource "snomed/init.sql"))
        migrations (str/split migration-sql #"\n--;--\n")]
    (doseq [migration migrations]
      (jdbc/execute! db [migration]))))

(defn get-copy-manager
  [conn]
  (org.postgresql.copy.CopyManager. (.unwrap ^java.sql.Connection conn org.postgresql.PGConnection)))

(defn load-file!
  "Load SNOMED CT tsv file.
  Table must exist before."
  [db table-name path]
  (let [connection (jdbc/get-connection db)
        reader (java.io.BufferedReader. (clojure.java.io/reader path))
        _ommitted_first-line (.readLine reader)]

    (.copyIn
      ^org.postgresql.copy.CopyManager
      (get-copy-manager connection) (format "COPY tmp_%s FROM STDIN" table-name) reader)

    (jdbc/execute! db [(format "INSERT INTO %s
                                SELECT *
                                FROM tmp_%s
                                ON CONFLICT DO NOTHING"
                               table-name table-name)])))

(defn load-files!
  "Load needed snomed files
  Concept, Description, Relationship, TextDefinition

  See load-file."
  [db files]
  (doseq [{path :path name :name} files
          :when (contains? #{"Concept" "Description" "Relationship" "TextDefinition"} name)]
    (load-file! db (str/lower-case name) path)))


(defn prepare-tables&build-indexes [db]
  ;;Concept table indexes
  (jdbc/execute! db ["DROP INDEX IF EXISTS concept_id; CREATE INDEX concept_id ON concept (id);"])
  (jdbc/execute! db ["ALTER TABLE concept ADD COLUMN IF NOT EXISTS display text"])
  (jdbc/execute! db ["ALTER TABLE concept ADD COLUMN IF NOT EXISTS definition text"])
  (jdbc/execute! db ["ALTER TABLE concept ADD COLUMN IF NOT EXISTS ancestors jsonb"])
  (jdbc/execute! db ["ALTER TABLE concept ADD COLUMN IF NOT EXISTS root jsonb"])
  (jdbc/execute! db ["ALTER TABLE concept ADD COLUMN IF NOT EXISTS designation jsonb"])

  ;;Description table indexes
  (jdbc/execute! db ["DROP INDEX IF EXISTS description_conceptid; CREATE INDEX description_conceptid ON description (conceptid);"])
  (jdbc/execute! db ["DROP INDEX IF EXISTS description_typeid; CREATE INDEX description_typeid ON description (typeid);"])

  ;;Relationship table keep only active & "is-a" relations
  (jdbc/execute! db ["DELETE FROM relationship WHERE typeid <> '116680003' OR active <> '1'"])
  (jdbc/execute! db ["VACUUM (FULL) relationship"])

  (jdbc/execute! db ["DROP INDEX IF EXISTS relationship_sourceid; CREATE INDEX relationship_sourceid ON relationship (sourceid);"])
  (jdbc/execute! db ["DROP INDEX IF EXISTS relationship_destinationid; CREATE INDEX relationship_destinationid ON relationship (destinationid);"])

  ;;sdl table indexes
  (jdbc/execute! db ["DROP INDEX IF EXISTS sdl_src; CREATE INDEX sdl_src ON sdl (src);"])
  (jdbc/execute! db ["DROP INDEX IF EXISTS sdl_dst; CREATE INDEX sdl_dst ON sdl (dst);"])
  (jdbc/execute! db ["DROP INDEX IF EXISTS sdl_src_dst; CREATE UNIQUE INDEX sdl_src_dst ON sdl USING btree (src, dst);"])

  )

(defn populate-sdl-table
  "For each concept find every ancestor and calculate distance between concept and this ancestor
  according to http://people.apache.org/~dongsheng/horak/100309_dag_structures_sql.pdf"
  [db]
  (jdbc/execute! db [
"WITH RECURSIVE bfs AS
  (SELECT destinationid src,
          sourceid dst,
          1 dist
   FROM relationship
   UNION SELECT b.src src,
                r.sourceid dst,
                b.dist + 1 dist
   FROM bfs b
   INNER JOIN relationship r ON r.destinationid = b.dst)
INSERT INTO sdl
SELECT src,
       dst,
       min(dist)
FROM bfs
GROUP BY src,
         dst;"]))

(defn populate-concept-table-with-ancestors
  "Calculate ancestors (based on sdl table) for each concept & create map with following strucutre
   {`code`: `distance-to-current-concept`}"
  [db]
  (jdbc/execute! db [
"UPDATE concept c
 SET    ancestors = (SELECT jsonb_object_agg(src, dist)
                     FROM   sdl m
                     WHERE  m.dst = c.id);"]))

(defn calculate-concept-roots [db]
  (jdbc/execute! db ["DROP INDEX IF EXISTS concept_ancestors; CREATE INDEX concept_ancestors ON concept USING gin(ancestors);"])

  (jdbc/execute! db [
                     "WITH roots AS
  (SELECT c.id cid,
          jsonb_agg(t.id) rids
   FROM concept t
   LEFT JOIN concept c ON c.ancestors @?? ('$.\"' || t.id || '\"')::jsonpath
   WHERE t.ancestors @> '{\"138875005\": 1}'::JSONB
   GROUP BY c.id)
UPDATE concept c
SET root = rids
FROM roots r
WHERE c.id = r.cid;"]))

(defn join-displays [db]
  (jdbc/execute! db ["
UPDATE concept c
SET display = d.term
FROM description d
WHERE d.typeid = '900000000000003001' AND d.active = '1' AND d.conceptid = c.id"]))

(defn join-designation [db {:as _cfg, :keys [join-original-language-as-designation]}]
  (jdbc/execute! db [(format "
UPDATE concept c
SET designation =
  (
   SELECT jsonb_agg(jsonb_build_object('language', d.languagecode, 'value', d.term) ORDER BY d.languagecode)
   FROM description d
   WHERE d.typeid = '900000000000003001'
     AND d.active = '1'
     AND d.conceptid = c.id
     %s)" (if-not join-original-language-as-designation
            "AND d.languagecode <> 'en'" ""))]))

(defn join-textdefinitions [db]
  (jdbc/execute! db ["
with aggs as (select conceptid cid, string_agg(term, '; ') saggs from textdefinition where active = '1' group by conceptid)
UPDATE concept c
SET definition = a.saggs
FROM aggs a
WHERE a.cid = c.id"]))


(defn populate-db-with-snomed-data!
  "`db` - JDBC Connection string
   `path` - path to unzipped SNOMED CT folder
   `cfg` - various extractor configs"
  [db path cfg]
  (let [sf (snomed-files path)]
    (ftr.utils.core/print-wrapper
     (init-db-tables db)
     (load-files! db sf)
     (prepare-tables&build-indexes db)
     (populate-sdl-table db)
     (populate-concept-table-with-ancestors db)
     (calculate-concept-roots db)
     (join-displays db)
     (join-designation db cfg)
     (join-textdefinitions db))))


(defmethod u/*fn ::create-value-set [cfg]
  {::result {:value-set
             (-> (:value-set cfg)
                 (assoc :resourceType "ValueSet")
                 (->> (merge {:status  "unknown"
                              :compose {:include [{:system (get-in cfg [:code-system :url])}]}})))}})


(defmethod u/*fn ::create-code-system [cfg]
  {::result {:code-system (-> (:code-system cfg)
                              (assoc :resourceType "CodeSystem")
                              (->> (merge {:status   "unknown"
                                           :content  "not-present"
                                           :valueSet (get-in cfg [:value-set :url])})
                                   (conj #{})))}})


(defmethod u/*fn ::populate-db-with-snomed-data [{:as cfg, :keys [source-url source-urls db]}]
  (populate-db-with-snomed-data! db (or source-urls source-url) cfg)
  {})


(defmethod u/*fn ::create-concepts-copy-out-obj [{:as _cfg,
                                                  :keys [db]
                                                  {:keys [value-set code-system]} ::result}]
  (let [connection (jdbc/get-connection db)
        pstmnt ^PreparedStatement (jdbc/prepare connection [(format "SELECT  jsonb_strip_nulls(jsonb_build_object(
                           'resourceType', 'Concept',
                           'deprecated', (not (active::boolean)),
                           'system', '%s',
                           'valueset', jsonb_build_array('%s'),
                           'code', id,
                           'display', display,
                           'designation', designation,
                           'ancestors', ancestors,
                           'property', jsonb_object_nullif(jsonb_build_object('roots', root)),
                           'definition', definition)) as ftr_concept FROM concept ORDER BY id"
                                                                    (:url (first code-system))
                                                                    (:url value-set))]
                                                {:fetch-size 1000})]
    {::result {:concepts pstmnt}}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::create-value-set
                       ::create-code-system
                       ::populate-db-with-snomed-data
                       ::create-concepts-copy-out-obj]
                      cfg)))
