(ns ftr.extraction.rxnorm
  (:require
   [clojure.java.io :as io]
   [next.jdbc :as jdbc]
   [dsql.pg :as dsql]
   [clojure.string :as str]
   [ftr.utils.unifn.core :as u]
   [ftr.utils.core])
  (:import
   [java.sql PreparedStatement]
   [java.io File]))


(defn q!
  "Executes a DSQL query on a JDBC connection.

   Arguments:
     `db` - JDBC connection string
     `query` - DSQL query map, example: {:select :* :from :mytable}"
  [db query ]
  (jdbc/execute! db (dsql/format query)))


(defn get&wrap-rxnorm-files
  "
  `path` - Path to the unzipped RxNorm bundle rrf directory.

  Description:
  Returns a list of RxNorm bundle files wrapped in objects with `:file`, `:file-name`, `:table-name` properties.

  File paths are constructed based on the provided `path`.
  If `path` is an absolute path, the resulting paths will also be absolute.

  The `:file-name` property will be used later for filtering the files required for actual processing.
  The `:table-name` property will be used later for specific table selection
  "
  [path]
  (->> (io/file path "rrf")
       (.listFiles)
       (mapv (fn [^File f]
               (let [file-name (.getName f)
                     table-name (-> file-name
                                    (str/split #"\." 2)
                                    first
                                    str/upper-case)]
                 {:file f
                  :file-name file-name
                  :table-name table-name})))))


(defn drop-table-if-exists
  "Drops the specified table from the database if it exists.

   Arguments:
   db         - the database connection to be used for executing the SQL command.
   table-name - the name of the table to be dropped, as a string."
  [db table-name]
  (q! db {:ql/type :pg/drop-table
          :table-name table-name
          :if-exists true}))


(def
  ^{:doc "A hardcoded table structure derived from Oracle DDL files found in the RxNorm bundle.
          We haven't found a straightforward method to automate DDL extraction."}
  tables-columns
  {:rxnconso-columns
   [["rxcui"    {:type "text"}]
    ["lat"      {:type "text"}]
    ["ts"       {:type "text"}]
    ["lui"      {:type "text"}]
    ["stt"      {:type "text"}]
    ["sui"      {:type "text"}]
    ["ispref"   {:type "text"}]
    ["rxaui"    {:type "text"}]
    ["saui"     {:type "text"}]
    ["scui"     {:type "text"}]
    ["sdui"     {:type "text"}]
    ["sab"      {:type "text"}]
    ["tty"      {:type "text"}]
    ["code"     {:type "text"}]
    ["str"      {:type "text"}]
    ["srl"      {:type "text"}]
    ["suppress" {:type "text"}]
    ["cvf"      {:type "text"}]
    ["empt"     {:type "text"}]
    ["idx"      {:type "serial"}]]
   :rxnrel-columns
   [["rxcui1"   {:type "text"}]
    ["rxaui1"   {:type "text"}]
    ["stype1"   {:type "text"}]
    ["rel"      {:type "text"}]
    ["rxcui2"   {:type "text"}]
    ["rxaui2"   {:type "text"}]
    ["stype2"   {:type "text"}]
    ["rela"     {:type "text"}]
    ["rui"      {:type "text"}]
    ["srui"     {:type "text"}]
    ["sab"      {:type "text"}]
    ["sl"       {:type "text"}]
    ["rg"       {:type "text"}]
    ["dir"      {:type "text"}]
    ["suppress" {:type "text"}]
    ["cvf"      {:type "text"}]
    ["empt"     {:type "text"}]
    ["idx"      {:type "serial"}]]
   :rxnsat-columns
   [["rxcui"    {:type "text"}]
    ["lui"      {:type "text"}]
    ["sui"      {:type "text"}]
    ["rxaui"    {:type "text"}]
    ["stype"    {:type "text"}]
    ["code"     {:type "text"}]
    ["atui"     {:type "text"}]
    ["satui"    {:type "text"}]
    ["atn"      {:type "text"}]
    ["sab"      {:type "text"}]
    ["atv"      {:type "text"}]
    ["suppress" {:type "text"}]
    ["cvf"      {:type "text"}]
    ["empt"     {:type "text"}]
    ["idx"      {:type "serial"}]]})


(defn prepare-table!
  "Prepares the table by dropping it if it exists and then creating it afresh.

  Arguments:
   db - the database connection to be used for executing the SQL commands."
  [db table-name table-columns]
  (drop-table-if-exists db table-name)
  (q! db
      {:ql/type :pg/create-table
       :table-name table-name
       :if-not-exists true
       :columns table-columns}))


(defn filter-out-known-files
  "Filters out specific object files that are essential for populating the RxNorm DB.

   Arguments
   rxnorm-files - collection of files constucted by `get&wrap-rxnorm-files` fn"
  [rxnorm-files]
  (let [known-files #{"RXNCONSO.RRF" "RXNREL.RRF" "RXNSAT.RRF"}]
    (into #{} (filter #(contains? known-files (:file-name %))) rxnorm-files)))


(defn get-copy-manager
  "Returns an instance of `org.postgresql.copy.CopyManager` for performing efficient bulk data copy operations in PostgreSQL.

  Arguments:
    `conn` - JDBC connection `object`."
  [conn]
  (org.postgresql.copy.CopyManager. (.unwrap ^java.sql.Connection conn org.postgresql.PGConnection)))


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


(defn load-rxnorm-data
  "Loads data into the database from RRF files present in the RxNorm bundle.

  Arguments:
  - `db`: A database connection string.
  - `path`: The file path to the directory containing RxNorm RRF files."
  [db path]
  (prepare-table! db "RXNCONSO" (:rxnconso-columns tables-columns))
  (prepare-table! db "RXNREL"   (:rxnrel-columns tables-columns))
  (prepare-table! db "RXNSAT"   (:rxnsat-columns tables-columns))
  (let [rxnorm-files (get&wrap-rxnorm-files path)
        known-rxnorm-files (filter-out-known-files rxnorm-files)]
    (doseq [{:keys [file table-name]} known-rxnorm-files]
      (let [reader (io/reader file)
            connection (jdbc/get-connection db)
            columns-spec  (butlast (get tables-columns (keyword (str/lower-case (str table-name "-columns")))))]

        (.copyIn
          ^org.postgresql.copy.CopyManager
          (get-copy-manager connection)
          (format "COPY %s (%s) FROM STDIN WITH DELIMITER '|' QUOTE AS E'\\b' CSV" table-name (str/join ", " (map first columns-spec)))
          reader)))

    (q! db {:ql/type :pg/delete
            :from :rxnconso
            :where ^:pg/op[:<> :sab "RXNORM"]}) ;; Codes with sab != RXNORM - requires tricky licensing process, so we omit them.

    (q! db {:ql/type :pg/delete
            :from :rxnrel
            :where ^:pg/op[:<> :sab "RXNORM"]})

    (q! db {:ql/type :pg/delete
            :from :rxnsat
            :where ^:pg/op[:<> :sab "RXNORM"]})

    (create-idx db :rxnconso "rxcui")
    (create-idx db :rxnconso "suppress")
    (create-idx db :rxnconso "tty")
    (create-idx db :rxnrel "rxcui1")
    (create-idx db :rxnrel "rxcui2")
    (create-idx db :rxnrel "rela")
    (create-idx db :rxnsat "rxcui")
    (create-idx db :rxnsat "atv")
    (create-idx db :rxnsat "atn")))


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


(defmethod u/*fn ::populate-db-with-rxnorm-data [{:as _cfg, :keys [source-url source-urls db]}]
  (load-rxnorm-data db (or source-urls source-url))
  {})

(def rxnsat-atn-collections
  {:all     ["AMBIGUITY_FLAG"
             "NDC"
             "ORIG_CODE"
             "ORIG_SOURCE"
             "RXN_ACTIVATED"
             "RXN_AI"
             "RXN_AM"
             "RXN_AVAILABLE_STRENGTH"
             "RXN_BN_CARDINALITY"
             "RXN_BOSS_FROM"
             "RXN_BOSS_STRENGTH_DENOM_UNIT"
             "RXN_BOSS_STRENGTH_DENOM_VALUE"
             "RXN_BOSS_STRENGTH_NUM_UNIT"
             "RXN_BOSS_STRENGTH_NUM_VALUE"
             "RXN_HUMAN_DRUG"
             "RXN_IN_EXPRESSED_FLAG"
             "RXN_OBSOLETED"
             "RXN_QUALITATIVE_DISTINCTION"
             "RXN_QUANTITY"
             "RXN_STRENGTH"
             "RXN_VET_DRUG"
             "RXTERM_FORM"]

   :multi   ["ORIG_SOURCE"
             "RXN_AM"
             "RXN_BOSS_FROM"
             "AMBIGUITY_FLAG"
             "RXN_AVAILABLE_STRENGTH"
             "RXN_OBSOLETED"
             "RXN_ACTIVATED"
             "ORIG_CODE"
             "RXN_AI"]

   :single  ["RXN_IN_EXPRESSED_FLAG"
             "RXN_HUMAN_DRUG"
             "RXN_BOSS_STRENGTH_DENOM_VALUE"
             "RXN_BOSS_STRENGTH_NUM_VALUE"
             "RXN_QUANTITY"
             "RXN_BOSS_STRENGTH_NUM_UNIT"
             "RXN_BOSS_STRENGTH_DENOM_UNIT"
             "RXN_BN_CARDINALITY"
             "RXN_STRENGTH"
             "RXN_VET_DRUG"
             "RXTERM_FORM"
             "RXN_QUALITATIVE_DISTINCTION"]})


(defmethod u/*fn ::create-concepts-copy-out-obj [{:as _cfg,
                                                  :keys [db]
                                                  {:keys [value-set code-system]} ::result}]
  (let [query {:ql/type :pg/cte
               :with {:preparations
                      {:ql/type :pg/select
                       :select {:suppress :suppress
                                :rxcui  :rxcui
                                :tty    [:pg/sql "jsonb_agg(tty order by CASE WHEN tty='SCD' THEN 110 WHEN tty='SCDG' THEN 120 WHEN tty='SCDF' THEN 130 WHEN tty='SCDC' THEN 140 WHEN tty='SBD' THEN 210 WHEN tty='SBDG' THEN 220 WHEN tty='SBDF' THEN 230 WHEN tty='SBDC' THEN 240 WHEN tty='MIN' THEN 310 WHEN tty='PIN' THEN 320 WHEN tty='IN' THEN 330 WHEN tty='GPCK' THEN 410 WHEN tty='BPCK' THEN 420 WHEN tty='PSN' THEN 510 WHEN tty='SY' THEN 520 WHEN tty='TMSY' THEN 530 WHEN tty='BN' THEN 610 WHEN tty='DF' THEN 710 WHEN tty='ET' THEN 720 WHEN tty='DFG' THEN 730 ELSE NULL END)"]
                                :displays [:pg/sql "jsonb_agg(str order by CASE WHEN tty='SCD' THEN 110 WHEN tty='SCDG' THEN 120 WHEN tty='SCDF' THEN 130 WHEN tty='SCDC' THEN 140 WHEN tty='SBD' THEN 210 WHEN tty='SBDG' THEN 220 WHEN tty='SBDF' THEN 230 WHEN tty='SBDC' THEN 240 WHEN tty='MIN' THEN 310 WHEN tty='PIN' THEN 320 WHEN tty='IN' THEN 330 WHEN tty='GPCK' THEN 410 WHEN tty='BPCK' THEN 420 WHEN tty='PSN' THEN 510 WHEN tty='SY' THEN 520 WHEN tty='TMSY' THEN 530 WHEN tty='BN' THEN 610 WHEN tty='DF' THEN 710 WHEN tty='ET' THEN 720 WHEN tty='DFG' THEN 730 ELSE NULL END)"]}
                       ;; ^--- RXCONSO might contain multiple entries for a single RXCUI value. These entries
                       ;;      represent various additional variations of the same atom. We empirically determine
                       ;;      the 'priorities' of such variations to select the primary variant for the 'display'
                       ;;      value, while preserving the additional variants in the 'other-display' property.
                       ;;      https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix5.html
                       :from :rxnconso
                       :group-by {:rxcui :rxcui :suppress :suppress}}}
               :select {:ql/type :pg/select
                        :select {:ftr_concept
                                 ^:pg/obj {:code :preparations.rxcui
                                           :display [:#>> :preparations.displays [0]]
                                           :resourceType "Concept"
                                           :_source "zen.fhir"
                                           :system (:url (first code-system))
                                           :valueset
                                           ^:pg/fn[:jsonb_build_array (:url value-set)]
                                           :property  ^:pg/fn [:jsonb_strip_nulls
                                                               [:||
                                                                ^:pg/obj {:other-display [:cond
                                                                                          [:= ^:pg/fn[:jsonb_array_length [:- :preparations.displays 0]] 0] nil
                                                                                          [:- :preparations.displays 0]]
                                                                          :suppressible-flag :preparations.suppress
                                                                          :tty :preparations.tty}

                                                                {:ql/type :pg/cte
                                                                 :with {:attrs
                                                                        {:ql/type :pg/select
                                                                         :select {:name :atn
                                                                                  :value ^:pg/fn[:jsonb_agg :atv]}
                                                                         :from :rxnsat
                                                                         :where [:and
                                                                                 [:= :rxcui :preparations.rxcui]
                                                                                 [:in :atn [:pg/inplace-params-list (:multi rxnsat-atn-collections)]]]
                                                                         :group-by :atn
                                                                         :union-all {:wha {:ql/type :pg/sub-select
                                                                                           :select {:name :atn
                                                                                                    :value ^:pg/fn[:to_jsonb :atv]}
                                                                                           :from :rxnsat
                                                                                           :where [:and
                                                                                                   [:= :rxcui :preparations.rxcui]
                                                                                                   [:in :atn [:pg/inplace-params-list (:single rxnsat-atn-collections)]]]}}}}
                                                                 :select {:ql/type :pg/select
                                                                          :select {:attrs_res [:pg/coalesce ^:pg/fn[:jsonb_object_agg ^:pg/fn[:lower :name] :value] ^:pg/obj {}]}
                                                                          :from :attrs}}

                                                                {:ql/type :pg/cte
                                                                 :with {:rels
                                                                        {:ql/type :pg/select
                                                                         :select {:relation :rela
                                                                                  :object
                                                                                  ^:pg/fn [:jsonb_agg
                                                                                           ^:pg/fn [:jsonb_build_object
                                                                                                    "code" :rxcui1
                                                                                                    "display" {:ql/type :pg/sub-select
                                                                                                               :select  :conso.str
                                                                                                               :from {:conso :rxnconso}
                                                                                                               :where [:and
                                                                                                                       [:not-in :conso.tty [:pg/inplace-params-list ["SY" "TMSY" "PSN"]]]
                                                                                                                       [:= :conso.rxcui :rxcui1]]}]]}
                                                                         :from :rxnrel
                                                                         :where [:= :rxcui2 :preparations.rxcui]
                                                                         :group-by :relation}}
                                                                 :select {:ql/type :pg/select
                                                                          :select {:rels_res [:pg/coalesce ^:pg/fn[:jsonb_object_agg :relation :object] ^:pg/obj {}]}
                                                                          :from :rels}}]]}}
                        :from :preparations
                        :order-by :rxcui}}
        _ (jdbc/execute! (jdbc/get-connection db) ["VACUUM FULL ANALYZE"])
        prepared-statement (jdbc/prepare
                             (jdbc/get-connection db)
                             (dsql/format query)
                             {:fetch-size 1000})]
    {::result {:concepts prepared-statement}}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::create-value-set
                       ::create-code-system
                       ::populate-db-with-rxnorm-data
                       ::create-concepts-copy-out-obj]
                      cfg)))
