(ns ftr.extraction.loinc
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [next.jdbc :as jdbc]
            [dsql.pg :as dsql])
  (:import [java.sql
            PreparedStatement]
           java.io.File))


(defn q!
  "Executes a DSQL query on a JDBC connection.

   Arguments:
     `db` - JDBC connection string
     `query` - DSQL query map, example: {:select :* :from :mytable}"
  [db query ]
  (jdbc/execute! db (dsql/format query) ))


(defn get-copy-manager
  "Returns an instance of `org.postgresql.copy.CopyManager` for performing efficient bulk data copy operations in PostgreSQL.
   Used in `prepare-loinc-table` to efficently upload loinc CSV artifacts.

  Arguments:
    `conn` - JDBC connection `object`."
  [conn]
  (org.postgresql.copy.CopyManager. (.unwrap ^java.sql.Connection conn org.postgresql.PGConnection)))


(defn prepare-loinc-table
  "Prepares a LOINC artifact tables in the given database using the provided CSV reader.

  Arguments:
    `db` - JDBC connection string.
    `table` - keyword representing the name of the table to be created.
    `path` - A path to the LOINC artifact."
  [db table path]
  (let [reader (io/reader path)
        data (csv/read-csv reader)
        header (first data)
        column-spec (mapv (fn [column] [column {:type "text"}]) header)
        connection (jdbc/get-connection db)
        copy-reader (java.io.BufferedReader. reader)
        table-name (name table)]

    (q! db {:ql/type :pg/drop-table
            :table-name table-name
            :if-exists true})

    (q! db {:ql/type :pg/create-table
            :table-name table-name
            :if-not-exists true
            :columns column-spec})

    (.copyIn
      ^org.postgresql.copy.CopyManager
      (get-copy-manager connection)
      (format "COPY %s FROM STDIN CSV" table-name) copy-reader)))


(defn create-idx
  "Creates an index on a specified column of a table in the given database.

  Arguments:
    `db` - JDBC connection string.
    `table` - Symbol or string representing the name of the table.
    `column` - Symbol or string representing the name of the column on which the index should be created."
  [db table column]
  (let [index-name (format "%s_%s_idx" (name table) (name column))]

    (q! db {:ql/type   :pg/drop-index
            :index     index-name
            :if-exists true})

    (q! db {:ql/type :pg/index
            :index   index-name
            :on      (name table)
            :expr    [[:pg/identifier (name column)]]})))


(defn create-idxs
  "Creates indexe(s) on specified column(s) of a table in the given database.

  Arguments:
    `db` - JDBC connection string.
    `table` - keyword representing the name of the table on which the indexes should be created.
    `columns` - A string or vector representing the name(s) of the column(s) on which the indexes should be created. "
  [db table columns]
  (cond
    (string? columns) (create-idx db table columns)
    (vector? columns) (doseq [column columns]
                        (create-idx db table column))))


(defn make-linguistic-variant-file-prefix
  "Creates a linguistic variant file prefix based on the provided language map.
   Used in `load-loinc-data` function to locate LinguisticVariant
   artifacts by attaching prefix: LinguisticVariants/<prefix>LinguisticVariants.csv

  Arguments:
    `{:as _lang-map, :keys [id lang country]}` - A map representing the language details. It should have the following keys:
      - `id` - The identifier of the linguistic variant. (maps to ID column in LinguisticVariants/LinguisticVariants.csv)
      - `lang` - The language code. (maps to ISO_LANGUAGE column in LinguisticVariants/LinguisticVariants.csv)
      - `country` - The country code. (maps to ISO_COUNTRY column in LinguisticVariants/LinguisticVariants.csv
"
  [{:as _lang-map,
    :keys [id lang country]}]
  (format "%s%s%s" lang country id))


(defn make-fhir-lang
  "Creates a lang code for concept.designation based on urn:ietf:bcp:47 CodeSystem rules.

  Arguments:
    `{:as _lang-map, :keys [_id lang country]}` - A map representing the language details. It should have the following keys:
      - `id` - ommited.
      - `lang` - The language code. (maps to ISO_LANGUAGE column in LinguisticVariants/LinguisticVariants.csv)
      - `country` - The country code. (maps to ISO_COUNTRY column in LinguisticVariants/LinguisticVariants.csv"
  [{:as _lang-map,
    :keys [_id lang country]}]
  (format "%s-%s" lang country))


(def
  ^{:private true
    :doc "LOINC Artifact to Table Name Mapping
          This map associates LOINC artifacts with their corresponding table names.
          It is used to establish a mapping between different artifacts and the tables in which their data should be stored."}
  loinc-artifact->table-name
  {"LoincTableCore/LoincTableCore.csv"                                        "loinc"
   "AccessoryFiles/PartFile/Part.csv"                                         "part"
   "AccessoryFiles/PartFile/LoincPartLink_Primary.csv"                        "partlink_primary"
   "AccessoryFiles/ComponentHierarchyBySystem/ComponentHierarchyBySystem.csv" "hierarchy"})


(def
  ^{:private true
    :doc
    "A mapping that associates table names with their corresponding column(s) on which index should be constructed."}
  table->index-column
  {"loinc"                  "LOINC_NUM"
   "part"                   "PartNumber"
   "partlink_primary"       "LoincNumber"
   "hierarchy"              ["IMMEDIATE_PARENT" "CODE"]})


(defn load-loinc-data
  "Loads LOINC data into the specified database.

  Arguments:
    `db` - JDBC connection string.
    `path` - String representing the base path to the LOINC data files.
    `langs` - Optional sequence of language maps. Each language map should have the following keys:
      - `id` - The identifier of the linguistic variant. (maps to ID column in LinguisticVariants/LinguisticVariants.csv)
      - `lang` - The language code. (maps to ISO_LANGUAGE column in LinguisticVariants/LinguisticVariants.csv)
      - `country` - The country code. (maps to ISO_COUNTRY column in LinguisticVariants/LinguisticVariants.csv"
  [db path langs]
  (let [path                       (fn [relative] (str path File/separatorChar relative))
        loinc-artifact->table-name (cond-> loinc-artifact->table-name
                                     (not-empty langs)
                                     (into (map (fn [lang-map]
                                                  (let [prefix (make-linguistic-variant-file-prefix lang-map)]
                                                    [(format "AccessoryFiles/LinguisticVariants/%sLinguisticVariant.csv" prefix)
                                                     prefix]))
                                                langs)))
        table->index-column        (cond-> table->index-column
                                     (not-empty langs)
                                     (into (map (fn [lang-map]
                                                  [(make-linguistic-variant-file-prefix lang-map)
                                                   "LOINC_NUM"])
                                                langs)))]
    (doseq [[file table] loinc-artifact->table-name
            :let         [filepath (path file)]]
      (prepare-loinc-table db table filepath)
      (create-idxs db table (get table->index-column table)))))


(def
  ^{:private true
    :doc "List to exclude main components and supplemenatry props"}
  unused-properties
  #{"loinc_num" "component" "property" "time_aspct" "system" "scale_typ" "method_typ"
    "versionlastchanged" "versionfirstreleased" "external_copyright_notice" "long_common_name"})


(defn get-base-properties
  "Extracts column names from the LOINC table that represent concept properties.
   Main component properties such as `property`, `component`, `time_aspct`, `scale-typ`, `system`, `method_typ` are excluded,
   as well as other insignificant properties like `versionlastchanged`.

   Arguments:
     `db` - JDBC connection string. "
  [db]
  (->> (q! db {:select :column_name
               :from   :information_schema.columns
               :where  [:and
                        [:= :table_name "loinc"]
                        [:not-in :column_name [:pg/params-list unused-properties]]]})
       (mapv (fn [{:columns/keys [column_name]}]
               (let [column-name' (keyword column_name)]
                 [column-name' column-name'])))
       (into {})))

(let [{:n/keys [a b c]} {:n/a 1 :n/b 2 :n/c 3}]
  [a b c])

(defn create-base-property-table!
  "Creates loinc_base_json table, where:
     `id` column - loinc code
     `property` column - json object with base-properties, example: {\"shortname\": \"value\"}
   Creates index on `id` column.

   Arguments:
     `db` - JDBC connection string."
  [db]
  (let [base-properties (get-base-properties db)
        idx-name :loinc_base_json_pkey]
    (q! db {:ql/type :pg/create-table-as
            :table   :loinc_base_json
            :select  {:ql/type :pg/select
                      :select  {:id       :loinc_num
                                :property (with-meta base-properties {:pg/obj true})}
                      :from    :loinc}})

    (q! db {:ql/type :pg/drop-index
            :index idx-name
            :if-exists true})

    (q! db {:ql/type :pg/index
            :unique true
            :index idx-name
            :on :loinc_base_json
            :expr [[:pg/identifier :id]]})))


(defn create-part-link-table
  "Collects `primary` part codes (codes that starts with LP- prefix) for every loinc node code (codes that starts with digit),
   Stores them in separate table.

   Arguments:
     `db` - JDBC connection string.
     `table-name` - string representing the name of the table on which the indexes should be created. "
  [db table-name]
  (q! db {:ql/type       :pg/create-table-as
          :if-not-exists true
          :table         (keyword (str table-name "_json"))
          :select
          {:ql/type :pg/cte
           :with
           {:parts
            {:ql/type :pg/select
             :select
             {:LoincNumber :LoincNumber
              :property
              ^:pg/fn[:lower ^:pg/fn[:replace :Property "http://loinc.org/property/" ""]]
              :property_object
              [:cond
               [:> ^:pg/fn[:count :PartNumber] 1]
               [:jsonb_agg
                ^:pg/obj{:code    :PartNumber
                         :display :PartName
                         :system  :PartCodeSystem}]
               ^:pg/obj{:code    :PartNumber
                        :display :PartName
                        :system  :PartCodeSystem}]}

             :from     (keyword table-name)
             :where    [:= :LinkTypeName "Primary"]
             :group-by {:1 :LoincNumber
                        :2 :property
                        :3 :PartNumber
                        :4 :PartName
                        :5 :PartCodeSystem}}}
           :select
           {:ql/type  :pg/select
            :select   {:LoincNumber :LoincNumber
                       :property    ^:pg/op [:- ^:pg/fn[:jsonb_object_agg
                                                        :property
                                                        :property_object]
                                             "category"]}
            :from     :parts
            :group-by {:1 :LoincNumber}}}})
  (create-idxs db table-name "LoincNumber"))


(defn create-core-concepts
  "Creates table with aidbox concept representation: https://docs.aidbox.app/modules-1/terminology/concept

   Arguments:
     `db` - JDBC connection string.
     `value-set` - loinc all-codes value-set resource, extract url to put as backref on concept."
  [db value-set]
  (q! db {:ql/type :pg/create-table-as
          :table   :loinc_concept
          :select
          {:ql/type   :pg/select
           :select
           {:LoincNumber :base.id
            :concept
            ^:pg/fn
            [:jsonb_strip_nulls
             ^:pg/obj
             {:id           [:|| "loinc-" :base.id]
              :code         :base.id
              :display
              ^:pg/fn[:coalesce
                      :loinc.LONG_COMMON_NAME
                      [:->> :base.property "display"]]
              :_source      "zen.fhir"
              :system       "http://loinc.org"
              :valueset
              ^:pg/fn[:jsonb_build_array (:url value-set)]
              :resourceType "Concept"
              :designation
              ^:jsonb/array[^:pg/obj {:language "en"
                                      :value ^:pg/fn[:coalesce
                                                     :loinc.LONG_COMMON_NAME
                                                     [:->> :base.property "display"]]}]
              :property
              [:||
               [:-
                ^:pg/fn[:coalesce :base.property ^:pg/obj{}]
                "display"]
               ^:pg/fn[:coalesce :prim.property ^:pg/obj{}]
               ^:pg/obj
               {:ancestors
                [:pg/parens {:ql/type :pg/select
                             :select  ^:pg/fn [:jsonb_object_agg :src :dist]
                             :from    {:m :sdl}
                             :where   [:= :m.dst :base.id]}]}]}]}
           :from      {:base :loinc_base_json}
           :left-join {:prim  {:table :partlink_primary_json
                               :on    [:= :prim.LoincNumber :base.id]}
                       :loinc {:table :loinc
                               :on    [:= :loinc.LOINC_NUM :base.id]}}
           :group-by  {:1 :base.id
                       :2 :loinc.LONG_COMMON_NAME
                       :3 :base.property
                       :4 :prim.property}}})

  (create-idx db "loinc_concept", "LoincNumber"))


(defn insert-part-concepts-to-base-json-table
  "Enrich loinc_base_json table with concepts from part table,
   to build aidbox concepts later from loinc_base_json table only.

   Arguments:
     `db` - JDBC connection string. "
  [db]
  (q! db {:ql/type :pg/insert-select
          :into    :loinc_base_json
          :select {:ql/type :pg/select
                   :select {:id :partnumber
                            :property ^:pg/obj {:status :status
                                                :display :partname
                                                :shortname :partdisplayname}}
                   :from :part}
          :on-conflict {:on [:id] :do :nothing}}))


(defn insert-remaining-part-concepts-to-base-json-table
  "Missed LP-% concepts described in hierarchy table by columns `CODE` and `CODE_TEXT`
   Enrich loinc_base_json table with concepts from hierarchy table,
   to build aidbox concepts later from loinc_base_json table only.

   Arguments:
     `db` - JDBC connection string."
  [db]
  (q! db {:ql/type :pg/insert-select
          :into    :loinc_base_json
          :select {:ql/type :pg/select
                   :select {:id :code
                            :property ^:pg/obj {:display :code_text}}
                   :from :hierarchy}
          :on-conflict {:on [:id] :do :nothing}}))


(defn create&populate-sdl-table
  "For each concept find every ancestor and calculate distance between concept and this ancestor
   according to http://people.apache.org/~dongsheng/horak/100309_dag_structures_sql.pdf

   Arguments:
     `db` - JDBC connection string."
  [db]
  (q! db {:ql/type :pg/delete
          :from :hierarchy
          :where [:= :immediate_parent ""]})
  (q! db {:ql/type    :pg/create-table
          :table-name :sdl
          :columns    {:src  {:type "text"}
                       :dst  {:type "text"}
                       :dist {:type "integer"}}})

  (q! db {:ql/type :pg/cte-recursive
          :with    {:bfs {:ql/type :pg/select
                          :select  {:src  :IMMEDIATE_PARENT
                                    :dst  :CODE
                                    :dist 1}
                          :union   {:1 {:ql/type :pg/select
                                        :select  {:src  :b.src
                                                  :dst  :r.CODE
                                                  :dist [:+ :b.dist 1]}
                                        :from    {:b :bfs}
                                        :join    {:r {:table :hierarchy
                                                      :on    [:= :r.IMMEDIATE_PARENT :b.dst]}}}}
                          :from    :hierarchy}}
          :select  {:ql/type :pg/insert-select
                    :into    :sdl
                    :select  {:select   {:dist ^:pg/fn[:min :dist] :dst :dst :src :src} :from :bfs
                              :group-by {:1 :src
                                         :2 :dst}}}})

  (create-idx db "sdl" "src")
  (create-idx db "sdl" "dst")

  (q! db {:ql/type :pg/drop-index
          :index :sdl_src_dst
          :if-exists true})
  (q! db {:ql/type :pg/index
          :index :sdl_src_dst
          :unique true
          :on :sdl
          :expr [[:pg/identifier :src] [:pg/identifier :dst]]}))


(defn join-designations [db langs]
  (doseq [lang-map langs]
    (let [prefix (make-linguistic-variant-file-prefix lang-map)]
      (q! db {:ql/type    :pg/create-table
              :table-name (keyword (format "translation_%s" prefix))
              :columns    {:loincnumber {:type "text"}
                           :designation {:type "text"}}} )


      (with-redefs
        [dsql.pg/keys-for-select
         [[:explain :pg/explain]
          [:select :pg/projection]
          [:select-distinct :pg/projection]
          [:from :pg/from]
          [:join :pg/join]
          [:left-join-lateral :pg/join-lateral]
          [:left-join :pg/left-join]
          [:left-outer-join :pg/left-outer-join]
          [:where :pg/and]
          [:group-by :pg/group-by]
          [:having :pg/having]
          [:window :pg/window]
          [:union :pg/union]
          [:union-all :pg/union-all]
          [:order-by :pg/order-by]
          [:limit :pg/limit]
          [:offset :pg/offset]
          [:fetch :pg/fetch]
          [:for :pg/for]]]
        (q! db {:ql/type :pg/cte
                :with
                (merge
                  {:concept_with_props_translations
                   {:ql/type :pg/select
                    :select {:compnum     :comp.loincnumber
                             :propnum     :prop.loincnumber
                             :sysnum      :sys.loincnumber
                             :methnum     :meth.loincnumber
                             :timenum     :time.loincnumber
                             :sclnum      :scl.loincnumber
                             :lnum        :lc.loincnumber
                             :display     :tt.long_common_name
                             :compdisplay :tt.component
                             :propdisplay :tt.property
                             :sysdisplay  :tt.system
                             :methdisplay :tt.method_typ
                             :timedisplay :tt.time_aspct
                             :scldisplay  :tt.scale_typ}
                    :from {:tt (keyword prefix)}
                    :join {:lc {:table :loinc_concept
                                :on   [:= :lc.loincnumber :tt.loinc_num]}}
                    :left-join
                    (->>
                      [[:comp :component]
                       [:prop :property]
                       [:sys  :system]
                       [:meth :method_typ]
                       [:time :time_aspct]
                       [:scl :scale_typ]]
                      (map
                        (fn [[alias property]]
                          [alias
                           {:table :loinc_concept
                            :on [:=
                                 [:#>> :lc.concept [:property property :code]]
                                 (keyword (str (name alias) ".loincnumber"))]}]))
                      (into {}))}
                   :l_insert
                   {:ql/type :pg/insert-select
                    :into (keyword (format "translation_%s" prefix))
                    :select
                    {:ql/type :pg/sub-select
                     :select
                     ^{:pg/projection :distinct}
                     {:loincnumber :lnum
                      :designation
                      [:cond
                       [:= :display ""]
                       ^:pg/fn[:concat_ws ":" :compdisplay
                               :propdisplay
                               :timedisplay
                               :sysdisplay
                               :scldisplay
                               :methdisplay]
                       :display]}
                     :from :concept_with_props_translations}}}
                  (->>
                    [[:cmp_insert :compnum :compdisplay]
                     [:prop_insert :propnum :propdisplay]
                     [:sys_insert :sysnum :sysdisplay]
                     [:meth_insert :methnum :methdisplay]
                     [:time_insert :timenum :timedisplay]
                     [:scl_insert :sclnum :scldisplay]]
                    (map
                      (fn [[alias number display]]
                        [alias
                         {:ql/type :pg/insert-select
                          :into (keyword (format "translation_%s" prefix))
                          :select {:select {:loincnumber number :designation display} :from :concept_with_props_translations}
                          }]))
                    (into {})))
                :select {:ql/type :pg/select :select 1}}))

      (q! db {:ql/type :pg/delete
              :from (keyword (format "translation_%s" prefix))
              :where [:or
                      [:= :designation ""]
                      [:= :loincnumber ""]
                      [:is :loincnumber nil]]})

      (create-idx db (format "translation_%s" prefix) "loincnumber")

      (q! db {:ql/type :pg/update
              :update  :loinc_concept
              :set     {:concept [:pg/jsonb_set :concept [:designation]
                                  [:||
                                   [:pg/coalesce
                                    [:-> :concept :designation]
                                    [:pg/cast "[]" :jsonb]]
                                   ^:pg/obj {:language (make-fhir-lang lang-map)
                                             :value    {:ql/type :pg/sub-select
                                                        :select  :designation
                                                        :from    (keyword (format "translation_%s" prefix))
                                                        :where   [:=
                                                                  (keyword (str (format "translation_%s" prefix) ".loincnumber"))
                                                                  :loinc_concept.loincnumber]
                                                        :limit   1}}]]}
              :from (keyword (format "translation_%s" prefix))
              :where [:=
                      (keyword (str (format "translation_%s" prefix) ".loincnumber"))
                      :loinc_concept.loincnumber]})

      ;; Cleanup translations table
      (q! db {:ql/type :pg/drop-table
              :table-name (keyword prefix)
              :if-exists true})

      (q! db {:ql/type :pg/drop-table
              :table-name (keyword (format "translation_%s" prefix))
              :if-exists true}))))


(defn create-indexes&populate-tables [db langs value-set]
  (doseq [table #{"loinc_concept" "partlink_primary_json" "loinc_base_json" "sdl"}]
    (q! db {:ql/type :pg/drop-table
            :table-name table
            :if-exists true}))

  (create-base-property-table! db)
  (create-part-link-table db "partlink_primary")
  (insert-part-concepts-to-base-json-table db)
  (insert-remaining-part-concepts-to-base-json-table db)
  (create&populate-sdl-table db)
  (create-core-concepts db value-set)
  (join-designations db langs))


(defn populate-db-with-loinc-data!
  "Connects to a database using the JDBC connection string `db`.
   Loads the LOINC bundle from the unzipped `path`.
   Translations are joined as designations to concepts.

   Arguments:
     `db`    - A JDBC connection string.
     `path`  - The path to the unzipped LOINC bundle.
     `value-set` - ValuSet to markup concepts with backref
     `langs` - A vector specifying the translations to be joined as designations.
               Each value in the vector should be a map consisting of the columns values
               from the LinguisticVariants.csv file: ISO_LANGUAGE, ISO_COUNTRY, ID.
               If empty, designations are ommited.
               Examples: [{:id \"8\",  :lang \"fr\", :country \"CA\"}
                          {:id \"18\", :lang \"fr\", :country \"FR\"}]"
  [db path value-set langs]
  (ftr.utils.core/print-wrapper
    (load-loinc-data db path langs)
    (create-indexes&populate-tables db langs value-set)))


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


(defmethod u/*fn ::populate-db-with-loinc-data [{:as _cfg, :keys [source-url source-urls db lang value-set]}]
  (populate-db-with-loinc-data! db (or source-urls source-url) value-set lang)
  {})


(defmethod u/*fn ::create-concepts-copy-out-obj [{:as _cfg,
                                                  :keys [db]}]
  (let [connection (jdbc/get-connection db)
        pstmnt ^PreparedStatement (jdbc/prepare connection ["SELECT concept as ftr_concept
                                                             FROM loinc_concept
                                                             ORDER BY loincnumber"]
                                                {:fetch-size 1000})]
    {::result {:concepts pstmnt}}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::create-value-set
                       ::create-code-system
                       ::populate-db-with-loinc-data
                       ::create-concepts-copy-out-obj]
                      cfg)))


(comment
  (populate-db-with-loinc-data!
    "jdbc:postgresql://localhost:5125/ftr?user=ftr&password=password"
    "/private/tmp/ftr/ci_pipelines/loinc/uncompressed-loinc"
    {:id "loinc-1"
     :resourceType "ValueSet"
     :version 1
     :compose { :include [{:system "http://loinc.org"}]}
     :status "active"
     :name "LOINC"
     :url "http://loinc.org/vs"}
    [{:id "8",  :lang "fr", :country "CA"}
     {:id "18", :lang "fr", :country "FR"}])

  )
