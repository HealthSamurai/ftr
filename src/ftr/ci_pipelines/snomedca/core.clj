(ns ftr.ci-pipelines.snomedca.core
  (:require [ftr.utils.unifn.core :as u]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.pprint]
            [clojure.java.shell :as shell]
            [ftr.ci-pipelines.snomed.db]
            [ftr.ci-pipelines.utils]))


(defmethod u/*fn ::build-ftr-cfg
  [{:as   _ctx,
    :keys [db ftr-path source-urls]}]
  {:cfg {:module            "snomedca"
         :source-urls       source-urls
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :snomed
         :extractor-options {:db          db
                             :code-system {:resourceType  "CodeSystem"
                                           :id            "snomedca"
                                           :url           "http://snomed.info/sct"
                                           :description   "SNOMEDCT CA edition"
                                           :content       "complete"
                                           :version       "20220930"
                                           :name          "SNOMEDCTCA"
                                           :publisher     "National Library of Medicine (NLM)"
                                           :status        "active"
                                           :caseSensitive true}
                             :value-set   {:id           "snomedca"
                                           :resourceType "ValueSet"
                                           :compose      {:include [{:system "http://snomed.info/sct"}]}
                                           :version      "20220930"
                                           :status       "active"
                                           :name         "SNOMEDCTCA"
                                           :url          "http://snomed.info/sct"}}}})


(defn get-zen-fhir-version! []
  (str/trim (:out (shell/sh "git" "describe" "--tags" "--abbrev=0" :dir "zen.fhir"))))


(defmethod u/*fn ::generate-snomed-zen-package
  [{:as   _ctx,
    :keys [snomed-working-dir-path]}]
  (when snomed-working-dir-path
    (spit (str snomed-working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str snomed-working-dir-path "/zrc/snomedca.edn"))
            (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns     'snomedca
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags         #{'zen.fhir/value-set}
                                                 :zen/desc         "Includes all concepts from SNOMEDCT CA edition snapshot. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                                 :zen.fhir/version (get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url         "http://snomed.info/sct"
                                                    :zen.fhir/content :bundled}}
                                                 :uri              "http://snomed.info/sct"
                                                 :version          "20220930"
                                                 :ftr
                                                 {:module      "snomedca"
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
   "/tmp/snomedca/"

   :gs-object-url
   "gs://ftr/snomedca"

   :gs-ftr-object-url
   "gs://ftr"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
    (doto (u/*apply [::build-ftr-cfg
                     #_:ftr.core/apply-cfg
                     :ftr.ci-pipelines.utils/upload-to-gcp-bucket
                     ::generate-snomed-zen-package]
                    cfg)
      (clojure.pprint/pprint))))


(comment
  (pipeline {:source-urls "/Users/ghrp/Downloads/SnomedCT_Canadian_EditionRelease_PRODUCTION_20220930T120000Z"})

  )
