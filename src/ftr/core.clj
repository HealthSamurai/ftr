(ns ftr.core
  (:require [ftr.extraction.core]
            [ftr.writer.core]
            [ftr.post-write-coordination.core]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [ftr.tag-merge.core]))

;; TODO:
;; MODULE NAME SHOULDN'T CONTAIN PERIODS ADD VALIDATION IN CONFIG SCHEMA


(defmethod u/*fn ::extract-terminology [{:as _ctx, :keys [cfg]}]
  {:extraction-result (ftr.extraction.core/extract cfg)})


(defmethod u/*fn ::write-terminology-file [ctx]
  {:write-result (ftr.writer.core/write-terminology-file ctx)})


(defmethod u/*fn ::shape-ftr-layout [{:as _ctx,
                                      :keys [cfg write-result]}]
  (let [tag (:tag cfg)
        from (get-in cfg [:merge :from])
        module-path (format "%s/%s" (:ftr-path cfg) (:module cfg))
        tags-path (format "%s/tags" module-path)
        vs-dir-path (format "%s/vs" module-path)

        temp-tf-file
        ^java.io.File
        (:terminology-file write-result)
        tf-name (some-> temp-tf-file (.getName))
        temp-tf-path (some-> temp-tf-file (.getAbsolutePath))

        value-set-name (ftr.utils.core/escape-url (get-in write-result [:value-set :url]))
        vs-name-path (some->> value-set-name (format "%s/%s" vs-dir-path))]

    (doseq [p [tags-path vs-dir-path vs-name-path]]
      (ftr.utils.core/create-dir-if-not-exists! p))

    {:ftr-layout
     {:module-path module-path
      :tags-path tags-path
      :vs-dir-path vs-dir-path
      :vs-name-path vs-name-path
      :temp-tf-path temp-tf-path
      :tf-path (format "%s/%s" vs-name-path tf-name)
      :tf-tag-path (format "%s/tag.%s.ndjson.gz" vs-name-path tag)
      :tag-index-path (format "%s/%s.ndjson.gz" tags-path tag)
      :from-tag-index-path (format "%s/%s.ndjson.gz" tags-path from)}}))


(defmethod u/*fn ::write-tag-index-hash [{:as _ctx, :keys [ftr-layout cfg]}]
  (let [{:keys [tag]}                cfg
        {:keys [tags-path
                tag-index-path]} ftr-layout
        tag-index-path               tag-index-path
        tag-index-side-file-path     (format "%s/%s.hash" tags-path tag)
        tag-index-hash               (ftr.utils.core/gzipped-file-content->sha256 tag-index-path)]
    (spit tag-index-side-file-path
          (str tag-index-hash \newline))))


(defmethod u/*fn ::feeder [{:as ctx, :keys [extraction-result]}]
  (or (not-empty
        (last
          (for [[vs-url tf] extraction-result]
            (u/*apply [::write-terminology-file
                       ::shape-ftr-layout
                       :ftr.post-write-coordination.core/coordinate]
                      (assoc ctx
                             :extraction-result tf
                             ::feeder {:vs-url vs-url})))))
      {}))


(defmethod u/*fn ::infer-commit-type [ctx]
  (assoc ctx ::commit-type (if (get-in ctx [:cfg :merge]) :tag-merge :append)))


(def flat-table-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def ig-pipeline
  [::extract-terminology
   ::feeder
   ::write-tag-index-hash])


(def snomed-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def tag-merge-pipeline
  [::shape-ftr-layout
   :ftr.tag-merge.core/merge-tag
   ::write-tag-index-hash])


(def serialized-objects-array-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def ftr-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def icd10-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def loinc-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(def rxnorm-pipeline
  [::extract-terminology
   ::write-terminology-file
   ::shape-ftr-layout
   :ftr.post-write-coordination.core/coordinate
   ::write-tag-index-hash])


(defmethod u/*fn ::select-ftr-pipeline [{:as _ctx,
                                         ::keys [commit-type]
                                         {:keys [source-type]} :cfg}]
  {::selected-pipeline
   (case [commit-type source-type]
     [:append :flat-table]
     flat-table-pipeline

     [:append :ig]
     ig-pipeline

     [:append :snomed]
     snomed-pipeline

     [:append :serialized-objects-array]
     serialized-objects-array-pipeline

     [:append :ftr]
     ftr-pipeline

     [:append :icd10]
     icd10-pipeline

     [:append :loinc]
     loinc-pipeline

     [:append :rxnorm]
     rxnorm-pipeline

     [:tag-merge nil]
     tag-merge-pipeline)})


(defmethod u/*fn ::apply-ftr-pipeline [{:as ctx,
                                        ::keys [selected-pipeline]}]
  (u/*apply selected-pipeline ctx))


(defn apply-cfg [ctx]
  (u/*apply [::infer-commit-type
             ::select-ftr-pipeline
             ::apply-ftr-pipeline] ctx))


(defmethod u/*fn ::apply-cfg [ctx]
  (apply-cfg ctx))


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
             ::feeder
             ::write-tag-index-hash]
            ctx))


(comment
  (def a
    (apply-cfg {:cfg {:module            "icd10"
                      :source-url         "/Users/ghrp/Desktop/ICD2023SOURCE"
                      :ftr-path          "/tmp/icd10"
                      :tag               "prod"
                      :source-type       :icd10
                      :extractor-options {:code-system {:id           "icd-10"
                                                        :url          "http://hl7.org/fhir/sid/icd-10"}
                                          :value-set   {:id           "icd-10"
                                                        :url          "http://hl7.org/fhir/ValueSet/icd-10"}}}}))


  (def b
    (apply-cfg {:cfg {:module            "icd10subset"
                      :source-url        "/tmp/icd10/icd10"
                      :ftr-path          "/tmp"
                      :tag               "prod"
                      :source-type       :ftr
                      :extractor-options {:target-tag "prod"
                                          :value-set
                                          {:compose      {:include [{:valueSet ["http://hl7.org/fhir/ValueSet/icd-10"]
                                                                     :filter [{:op       "is-a"
                                                                               :property "concept"
                                                                               :value    "S12.00"}]}]}
                                           :description  "icd10 microsubset"
                                           :id           "icd10microsubset"
                                           :name         "ICD10MICROSUBSET"
                                           :resourceType "ValueSet"
                                           :status       "active"
                                           :url          "http://icd10microsubset.info"}}}}))


  )
