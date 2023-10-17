(ns ftr.ci-pipelines.snomed.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [ftr.core]
            [ftr.ci-pipelines.snomed.db]
            [clojure.pprint]
            [org.httpkit.client :as http]
            [ftr.ci-pipelines.utils]
            [hickory.core]
            [hickory.select :as s])
  (:import java.io.File))


(defn get-html-text-by-url [url]
  (-> url
      (http/get)
      (deref)
      :body))


(defmethod u/*fn ::get-latest-snomed-info!
  ([{:as _ctx, :keys [version-page-url complete-download-url-format api-key]}]
   (let [html-text (slurp version-page-url)
         parsed-html-text (-> html-text
                              (hickory.core/parse)
                              (hickory.core/as-hickory))
         download-url (-> (s/select ;; TODO request guide from UMLS about automated SNOMED downloads, this solution is SUPER fragile
                            (s/and
                              (s/tag :a)
                              (s/find-in-text #"Download Now!"))
                            parsed-html-text)
                          first
                          :attrs
                          :href)]
     {:snomed-info
      {:snomed-zip-url download-url
       :version (-> download-url
                    (str/split #"/")
                    (last)
                    (str/split #"_")
                    (last)
                    (str/split #"T")
                    (first))
       :complete-download-url (format complete-download-url-format download-url api-key)}})))


(def resources-to-load
  #{"Concept" "Description" "Relationship" "TextDefinition"})


(defn split-file-path [path]
  (str/split path (java.util.regex.Pattern/compile (str (File/separatorChar)))))


(defn is-terminology-snapshot-entry? [entry]
  (->> entry
       (split-file-path)
       (butlast)
       (take-last 2)
       (= '("Snapshot" "Terminology"))))


(defn entry->resource-to-load? [entry]
  (-> entry
      (split-file-path)
      (last)
      (str/split #"_")
      (second)
      (resources-to-load)))


(defmethod u/*fn ::write-snomed-snapshot-terminology-folder!
  [{:as _ctx,
    :keys [extracted-snomed-out-dir]
    {:keys [complete-download-url]} :snomed-info}]
  (with-open [^java.util.zip.ZipInputStream
              zip-stream
              (-> complete-download-url
                  (io/input-stream)
                  (java.util.zip.ZipInputStream.))]
    (loop [entry
           ^java.util.zip.ZipEntry
           (.getNextEntry zip-stream)

           loaded-resources #{}

           last-matched-entry-name ""]
      (if (and entry
               (not= loaded-resources resources-to-load))
        (let [entry-name (.getName entry)
              resource (entry->resource-to-load? entry-name)
              terminology-related-files-matcher (every-pred is-terminology-snapshot-entry?
                                                            (constantly resource))
              entry-name-match? (terminology-related-files-matcher entry-name)]
          (when entry-name-match?
            (let [save-path (str extracted-snomed-out-dir File/separatorChar entry-name)
                  out-file (io/file save-path)
                  parent-dir (.getParentFile out-file)]
              (when-not (.exists parent-dir) (.mkdirs parent-dir))
              (clojure.java.io/copy zip-stream out-file)))
          (recur (.getNextEntry zip-stream)
                 (cond-> loaded-resources entry-name-match? (conj resource))
                 entry-name))
        {:snomed-info
         {:snomed-folder-name (first (split-file-path last-matched-entry-name))}}))))


(defmethod u/*fn ::build-ftr-cfg
  [{:as _ctx,
    :keys [db ftr-path extracted-snomed-out-dir
           join-original-language-as-designation]
    {:keys [version snomed-folder-name]} :snomed-info}]
  {:cfg {:module            "snomed"
         :source-url        (str extracted-snomed-out-dir File/separatorChar snomed-folder-name)
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :snomed
         :extractor-options {:join-original-language-as-designation
                             join-original-language-as-designation
                             :db db
                             :code-system {:resourceType "CodeSystem"
                                           :id "snomedct"
                                           :url "http://snomed.info/sct"
                                           :description "SNOMEDCT US edition"
                                           :content "complete"
                                           :version version
                                           :name "SNOMEDCT"
                                           :publisher "National Library of Medicine (NLM)"
                                           :status "active"
                                           :caseSensitive true}
                             :value-set   {:id "snomedct"
                                           :resourceType "ValueSet"
                                           :compose { :include [{:system "http://snomed.info/sct"}]}
                                           :version version
                                           :status "active"
                                           :name "SNOMEDCT"
                                           :description "Includes all concepts from SNOMEDCT US edition snapshot. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                           :url "http://snomed.info/sct"}}}})


(defmethod u/*fn ::generate-snomed-zen-package
  [{:as _ctx,
    :keys [working-dir-path]
    {:keys [version]} :snomed-info}]
  (when working-dir-path
    (spit (str working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str working-dir-path "/zrc/snomed.edn"))
                (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns 'snomed
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags #{'zen.fhir/value-set}
                                                 :zen/desc "Includes all concepts from SNOMEDCT US edition snapshot. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                                 :zen.fhir/version (ftr.ci-pipelines.utils/get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url "http://snomed.info/sct"
                                                    :zen.fhir/content :bundled}}
                                                 :uri "http://snomed.info/sct"
                                                 :version version
                                                 :ftr
                                                 {:module "snomed"
                                                  :source-url "https://storage.googleapis.com"
                                                  :ftr-path "ftr"
                                                  :source-type :cloud-storage
                                                  :tag "prod"}}})))))


(def config-defaults
  {:version-page-url
   "https://www.nlm.nih.gov/healthit/snomedct/us_edition.html"

   :download-url-regex
   #"url=(https://download.nlm.nih.gov/mlb/utsauth/USExt/.+?\.zip)"

   :complete-download-url-format
   "https://uts-ws.nlm.nih.gov/download?url=%s&apiKey=%s"

   :db
   ftr.ci-pipelines.snomed.db/conn-str

   :ftr-path
   "/tmp/ftr/"

   :extracted-snomed-out-dir
   "/tmp/extracted-snomed"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
    (doto (u/*apply [:ftr.ci-pipelines.utils/download-previous-ftr-version!
                    ::get-latest-snomed-info!
                    ::write-snomed-snapshot-terminology-folder!
                    ::build-ftr-cfg
                    :ftr.core/apply-cfg
                    :ftr.ci-pipelines.utils/upload-to-gcp-bucket
                    ::generate-snomed-zen-package
                    :ftr.ci-pipelines.utils/push-zen-package
                    :ftr.ci-pipelines.utils/send-tg-notification]
                    cfg)
      (clojure.pprint/pprint))))
