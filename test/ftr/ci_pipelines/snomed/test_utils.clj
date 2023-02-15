(ns ftr.ci-pipelines.snomed.test-utils
  (:require  [clojure.test :as t]
             [ftr.test-utils :as test-utils]
             [hiccup.page]
             [clojure.java.io :as io]))


(def concept-content "id	effectiveTime	active	moduleId	definitionStatusId
100005	20020131	0	900000000000207008	900000000000074008
101009	20020131	1	900000000000207008	900000000000074008
102002	20020131	1	900000000000207008	900000000000074008
103007	20020131	1	900000000000207008	900000000000074008
104001	20020131	1	900000000000207008	900000000000073002")


(def concept-description "id	effectiveTime	active	moduleId	conceptId	languageCode	typeId	term	caseSignificanceId
208187016	20020131	1	900000000000207008	100005	en	900000000000013009	SNOMED RT Version 1.0 Concept	900000000000017005
208188014	20020131	1	900000000000207008	100005	en	900000000000013009	SNOMED RT Concept	900000000000017005
214164012	20020131	0	900000000000207008	100005	en	900000000000013009	SNOMED RT Version 1.1 Concept	900000000000017005
513543016	20080731	0	900000000000207008	100005	en	900000000000003001	SNOMED RT Concept	900000000000017005
2709997016	20080731	1	900000000000207008	100005	en	900000000000003001	SNOMED RT Concept (special concept)	900000000000017005
1305019	20070731	1	900000000000207008	101009	en	900000000000013009	Quilonia ethiopica	900000000000017005
525331019	20070731	1	900000000000207008	101009	en	900000000000003001	Quilonia ethiopica (organism)	900000000000017005
1306018	20020131	1	900000000000207008	102002	en	900000000000013009	Hemoglobin Okaloosa	900000000000017005
1307010	20020131	1	900000000000207008	102002	en	900000000000013009	Hb 48(CD7), Leu-arg	900000000000017005
243386019	20020131	1	900000000000207008	102002	en	900000000000013009	Haemoglobin Okaloosa	900000000000020002
536464016	20030731	1	900000000000207008	102002	en	900000000000003001	Hemoglobin Okaloosa (substance)	900000000000017005
1308017	20090131	1	900000000000207008	103007	en	900000000000013009	Squirrel fibroma virus	900000000000017005
547184017	20090131	1	900000000000207008	103007	en	900000000000003001	Squirrel fibroma virus (organism)	900000000000017005
1309013	20170731	1	900000000000207008	104001	en	900000000000013009	Excision of lesion of patella	900000000000448009
1310015	20170731	1	900000000000207008	104001	en	900000000000013009	Local excision of lesion or tissue of patella	900000000000448009
557742014	20170731	1	900000000000207008	104001	en	900000000000003001	Excision of lesion of patella (procedure)	900000000000448009")


(def concept-relationships " id	effectiveTime	active	moduleId	sourceId	destinationId	relationshipGroup	typeId	characteristicTypeId	modifierId
1399025	20020131	1	900000000000207008	101009	44577004	0	116680003	900000000000011006	900000000000451002
2759027	20020131	1	900000000000207008	102002	40830007	0	116680003	900000000000011006	900000000000451002
6099021	20020131	1	900000000000207008	103007	71511002	0	116680003	900000000000011006	900000000000451002
829441022	20140131	0	900000000000207008	62379007	103007	0	246075003	900000000000011006	900000000000451002
10437024	20200131	1	900000000000207008	104001	125675003	0	116680003	900000000000011006	900000000000451002
10438025	20020131	1	900000000000207008	104001	68471001	0	116680003	900000000000011006	900000000000451002
441736025	20020731	0	900000000000207008	104001	255609007	0	260858005	900000000000011006	900000000000451002
441737023	20050131	0	900000000000207008	104001	64234005	1	363704007	900000000000011006	900000000000451002
1671571024	20020731	1	900000000000207008	25522008	104001	0	116680003	900000000000011006	900000000000451002
1716441029	20080731	0	900000000000207008	104001	129304002	1	260686004	900000000000011006	900000000000451002
1716442020	20080731	0	900000000000207008	104001	49755003	1	363700003	900000000000011006	900000000000451002
2650051028	20070131	0	900000000000207008	104001	363110002	0	116680003	900000000000011006	900000000000451002
2707828022	20080731	0	900000000000207008	104001	64234005	1	405813007	900000000000011006	900000000000451002
2763935024	20210930	0	900000000000207008	104001	373196008	0	116680003	900000000000011006	900000000000451002
2763936020	20200131	0	900000000000207008	104001	128995005	0	116680003	900000000000011006	900000000000451002
2763937027	20210930	0	900000000000207008	104001	129203006	0	116680003	900000000000011006	900000000000451002
2763938021	20200131	0	900000000000207008	104001	6240004	0	116680003	900000000000011006	900000000000451002
3391975020	20150131	0	900000000000207008	104001	49755003	1	363700003	900000000000011006	900000000000451002
3391976021	20150131	1	900000000000207008	104001	64234005	2	405813007	900000000000011006	900000000000451002
3391977028	20150131	1	900000000000207008	104001	129304002	2	260686004	900000000000011006	900000000000451002
5973268022	20150131	1	900000000000207008	104001	52988006	2	363700003	900000000000011006	900000000000451002")


(def concept-text-definition "id	effectiveTime	active	moduleId	conceptId	languageCode	typeId	term	caseSignificanceId")


(def zip-entries
  [["Snapshot/Terminology/sct2_Concept_Snapshot_identifier_date.txt" concept-content]
   ["Snapshot/Terminology/sct2_Description_Snapshot-identifier_date.txt" concept-description]
   ["Snapshot/Terminology/sct2_Relationship_Snapshot-identifier_date.txt" concept-relationships]
   ["Snapshot/Terminology/sct2_TextDefinition_Snapshot-identifier_date.txt" concept-text-definition]])


(defn create-mock-snomed-bundle [path-to-zip-archive]
  (with-open [zip-stream
              ^java.util.zip.ZipOutputStream
              (-> path-to-zip-archive
                  (io/output-stream)
                  (java.util.zip.ZipOutputStream.))]
    (doseq [[path content] zip-entries]
      (let [zip-entry (java.util.zip.ZipEntry. path)]
        (.putNextEntry zip-stream zip-entry)
        (io/copy content zip-stream)
        (.closeEntry zip-stream)))))


(def mock-endpoints
  {:version-info "/snomed-version-info.html"
   :archive      "/download-archive"})


(defn generate-snomed-version-info-html-with-download-url
  []
  (str "url=" (:archive mock-endpoints)))


(defn mock-handler [req]
  (condp = (:uri req)
    (:version-info mock-endpoints)
    {:status  200
     :body    (generate-snomed-version-info-html-with-download-url)
     :headers {"Content-Type" "text/html; charset=utf-8"}}

    (:archive mock-endpoints)
    (let [{{:keys [path]} :params} req]
      {:status 200
       :body (io/input-stream path)})

    {:status 404}))


(defn start-mock-server [& [opts]]
  (test-utils/start-mock-server #'mock-handler opts))


(def mock-server-url test-utils/mock-server-url)


(comment
  (def mock-server (start-mock-server))

  (mock-server)

  nil)
