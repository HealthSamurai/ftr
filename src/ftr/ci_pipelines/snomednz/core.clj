(ns ftr.ci-pipelines.snomednz.core
  (:require [ftr.utils.unifn.core :as u]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.pprint]
            [clojure.java.shell :as shell]
            [ftr.ci-pipelines.snomed.db]))


(defmethod u/*fn ::build-ftr-cfg
  [{:as   _ctx,
    :keys [db ftr-path source-urls]}]
  {:cfg {:module            "snomednz"
         :source-urls       source-urls
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :snomed
         :extractor-options {:db          db
                             :code-system {:resourceType  "CodeSystem"
                                           :id            "snomednz"
                                           :url           "http://snomed.info/sct"
                                           :description   "SNOMEDCT NZ edition"
                                           :content       "complete"
                                           :version       "20221001"
                                           :name          "SNOMEDCTNZ"
                                           :publisher     "National Library of Medicine (NLM)"
                                           :status        "active"
                                           :caseSensitive true}
                             :value-set   {:id           "snomednz"
                                           :resourceType "ValueSet"
                                           :compose      { :include [{:system "http://snomed.info/sct"}]}
                                           :version      "20221001"
                                           :status       "active"
                                           :name         "SNOMEDCTNZ"
                                           :url          "http://snomed.info/sct"}}}})


(defn get-zen-fhir-version! []
  (str/trim (:out (shell/sh "git" "describe" "--tags" "--abbrev=0" :dir "zen.fhir"))))


(defmethod u/*fn ::generate-snomed-zen-package
  [{:as   _ctx,
    :keys [snomed-working-dir-path]}]
  (when snomed-working-dir-path
    (spit (str snomed-working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str snomed-working-dir-path "/zrc/snomednz.edn"))
            (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns     'snomednz
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags         #{'zen.fhir/value-set}
                                                 :zen/desc         "Includes all concepts from SNOMEDCT NZ edition snapshot. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                                 :zen.fhir/version (get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url         "http://snomed.info/sct"
                                                    :zen.fhir/content :bundled}}
                                                 :uri              "http://snomed.info/sct"
                                                 :version          "20221001"
                                                 :ftr
                                                 {:module      "snomednz"
                                                  :source-url  "https://storage.googleapis.com"
                                                  :ftr-path    "ftr"
                                                  :source-type :cloud-storage
                                                  :tag         "prod"}}})))))


(def config-defaults
  {:db
   ftr.ci-pipelines.snomed.db/conn-str

   :ftr-path
   "/tmp/ftr/"

   :extracted-snomed-out-dir
   "/tmp/extracted-snomed"

   :snomed-working-dir-path
   "/tmp/snomednz/"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
    (doto (u/*apply [#_::build-ftr-cfg
                     #_:ftr.core/apply-cfg
                     #_:ftr.ci-pipelines.snomed.core/upload-to-gcp-bucket
                     ::generate-snomed-zen-package]
                    cfg)
      (clojure.pprint/pprint))))
