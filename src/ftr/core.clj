(ns ftr.core
  (:require [clojure.java.io :as io]
            [ftr.extraction.core]
            [ftr.writer.core]
            [ftr.ingestion-coordinator.core]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]))


(defmethod u/*fn ::extract-terminology [{:as _ctx, :keys [cfg]}]
  {:extraction-result (ftr.extraction.core/extract cfg)})


(defmethod u/*fn ::write-terminology-file [ctx]
  {:write-result (ftr.writer.core/write-terminology-file ctx)})


(defmethod u/*fn ::shape-ftr-layout [{:as _ctx, :keys [cfg write-result]}]
  (let [tag (:tag cfg)
        ?new-tag (get-in cfg [:move-tag :new-tag])
        ?old-tag (get-in cfg [:move-tag :old-tag])
        module-path (format "%s/%s" (:ftr-path cfg) (:module cfg))
        tags-path (format "%s/tags" module-path)
        vs-dir-path (format "%s/vs" module-path)
        value-set-name (ftr.utils.core/escape-url (get-in write-result [:value-set :url]))
        temp-tf-file (:terminology-file write-result)
        tf-name (.getName temp-tf-file)
        temp-tf-path (.getAbsolutePath temp-tf-file)
        vs-name-path (format "%s/%s" vs-dir-path value-set-name)]

    (doseq [p [tags-path vs-dir-path vs-dir-path vs-name-path]]
      (ftr.utils.core/create-dir-if-not-exists! p))

    {:ftr-layout
     {:module-path module-path
      :tags-path tags-path
      :vs-dir-path vs-dir-path
      :vs-name-path vs-name-path
      :temp-tf-path temp-tf-path
      :tf-path (format "%s/%s" vs-name-path tf-name)
      :tf-tag-path (format "%s/tag.%s.ndjson.gz" vs-name-path tag)
      :old-tf-tag-path (when ?new-tag (format "%s/tag.%s.ndjson.gz" vs-name-path ?old-tag))
      :tag-index-path (format "%s/%s.ndjson.gz" tags-path tag)
      :new-tag-index-path (when ?new-tag (format "%s/%s.ndjson.gz" tags-path ?new-tag))}}))


(defmethod u/*fn ::write-tag-index-hash [{:as _ctx, :keys [ftr-layout cfg]}]
  (let [{:keys [tag]}                cfg
        {:keys [tags-path
                tag-index-path
                new-tag-index-path]} ftr-layout
        tag-index-path               (or new-tag-index-path tag-index-path)
        tag-index-side-file-path     (format "%s/%s.hash" tags-path tag)
        tag-index-hash               (ftr.utils.core/gzipped-file-content->sha256 tag-index-path)]
    (spit tag-index-side-file-path
          (str tag-index-hash \newline))))


(defmethod u/*fn ::feeder [{:as ctx, :keys [extraction-result]
                            {:keys [ftr-path]} :cfg}]
  (last
   (for [[vs-url tf] extraction-result]
     (u/*apply [::write-terminology-file
                ::shape-ftr-layout
                :ftr.ingestion-coordinator.core/ingest-terminology-file]
               (assoc ctx
                      :extraction-result tf
                      ::feeder {:vs-url vs-url})))))


(defmulti apply-cfg
  (fn [{:as _ctx, {:keys [source-type]} :cfg}]
    source-type))


(defmethod apply-cfg :flat-table [ctx]
  (u/*apply [::extract-terminology
             ::write-terminology-file
             ::shape-ftr-layout
             :ftr.ingestion-coordinator.core/ingest-terminology-file
             ::write-tag-index-hash] ctx))


(defmethod apply-cfg :ig [ctx]
  (u/*apply [::extract-terminology
             ::feeder
             ::write-tag-index-hash] ctx))


(defmethod apply-cfg :snomed [ctx]
  (u/*apply [::extract-terminology
             ::write-terminology-file
             ::shape-ftr-layout
             :ftr.ingestion-coordinator.core/ingest-terminology-file
             ::write-tag-index-hash] ctx))


;; `apply-cfg` split into 2 separate processes, is used in zen-lang/fhir CI pipeline
(defmulti extract
  (fn [{:as _ctx, {:keys [source-type]} :cfg}]
    source-type))


(defmethod extract :igs [ctx]
  (u/*apply [::extract-terminology]
            ctx))


(defmulti spit-ftr
  (fn [{:as _ctx, {:keys [source-type]} :cfg}]
    source-type))


(defmethod u/*fn ::select-package-valuesets [{:as _ctx, :keys [extraction-result]
                                              {:keys [vs-urls]} :cfg}]
  ^:non-deep-merge {:extraction-result (select-keys extraction-result vs-urls)})


(defmethod spit-ftr :igs [ctx]
  (u/*apply [::select-package-valuesets
             ::feeder]
            ctx))


(comment
  (apply-cfg {:cfg {:module            "snomed"
                    :source-url        "/Users/ghrp/Downloads/SnomedCT_USEditionRF2_PRODUCTION_20220301T120000Z"
                    :ftr-path          "/tmp"
                    :tag               "init"
                    :source-type       :snomed
                    :extractor-options {:db "jdbc:postgresql://localhost:55002/postgres?user=postgres&password=postgrespw"
                                        :code-system {:resourceType "CodeSystem"
                                                      :id "snomedct"
                                                      :url "http://snomed.info/sct"
                                                      :date "2022-03-01"
                                                      :description "SNOMEDCT US edition snapshot US1000124"
                                                      :content "complete"
                                                      :version "03"
                                                      :name "SNOMEDCT"
                                                      :publisher "National Library of Medicine (NLM)"
                                                      :status "active"
                                                      :caseSensitive true}
                                        :value-set   {:id "snomedct"
                                                      :resourceType "ValueSet"
                                                      :compose { :include [{:system "http://snomed.info/sct"}]}
                                                      :date "2022-03-01"
                                                      :version "03"
                                                      :status "active"
                                                      :name "SNOMEDCT"
                                                      :description "Includes all concepts from SNOMEDCT US edition snapshot US1000124. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                                      :url "http://snomed.info/sct"}}}})



  )
