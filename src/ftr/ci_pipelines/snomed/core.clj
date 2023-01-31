(ns ftr.ci-pipelines.snomed.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [ftr.core]
            [ftr.ci-pipelines.snomed.db]
            [clojure.pprint]
            [clojure.java.shell :as shell]
            [org.httpkit.client :as http])
  (:import java.io.File))


(defn get-html-text-by-url [url]
  (-> url
      (http/get)
      (deref)
      :body))


(defmethod u/*fn ::get-latest-snomed-info!
  ([{:as _ctx, :keys [version-page-url download-url-regex complete-download-url-format api-key]}]
   (let [html-text (get-html-text-by-url version-page-url)
         [_ download-url] (re-find download-url-regex
                                   html-text)]
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
  (with-open [zip-stream
              ^java.util.zip.ZipInputStream
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
    :keys [db ftr-path extracted-snomed-out-dir]
    {:keys [version snomed-folder-name]} :snomed-info}]
  {:cfg {:module            "snomed"
         :source-url        (str extracted-snomed-out-dir File/separatorChar snomed-folder-name)
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :snomed
         :extractor-options {:db db
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


(defmethod u/*fn ::download-previous-snomed-ftr-version
  [{:as _ctx, :keys [snomed-object-url ftr-path]}]
  (when snomed-object-url
    (let [{:as _execution-result
           :keys [exit err]}
          (shell/sh "gsutil" "cp" "-r" snomed-object-url ftr-path)]
      (when-not (= exit 0)
        {::u/status :error
         ::u/message err}))))


(defmethod u/*fn ::upload-to-gcp-bucket
  [{:as _ctx,
    :keys [ftr-object-url],
    {:keys [ftr-path module]}:cfg}]
  (when ftr-object-url
    (let [{:as _execution-result
           :keys [exit err]}
          (shell/sh "gsutil" "rsync" "-r"
                    (str ftr-path \/ module)
                    (str ftr-object-url \/ module))]
      (when-not (= exit 0)
        {::u/status :error
         ::u/message err}))))


(defn get-zen-fhir-version! []
  (str/trim (:out (shell/sh "git" "describe" "--tags" "--abbrev=0" :dir "zen.fhir"))))


(defmethod u/*fn ::generate-snomed-zen-package
  [{:as _ctx,
    :keys [snomed-working-dir-path]
    {:keys [version]} :snomed-info}]
  (when snomed-working-dir-path
    (spit (str snomed-working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str snomed-working-dir-path "/zrc/snomed.edn"))
                (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns 'snomed
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags #{'zen.fhir/value-set}
                                                 :zen/desc "Includes all concepts from SNOMEDCT US edition snapshot. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                                 :zen.fhir/version (get-zen-fhir-version!)
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


(defmethod u/*fn ::push-zen-package
  [{:as _ctx,
    :keys [snomed-working-dir-path]}]
  (when snomed-working-dir-path
    (loop [commands [["git" "add" "--all" :dir snomed-working-dir-path]
                     ["git" "commit" "-m" "Update zen package" :dir snomed-working-dir-path]
                     ["git" "push" "-u" "origin" "main" :dir snomed-working-dir-path]]]
      (when-not (nil? commands)
        (let [{:as _executed-command,
               :keys [exit err]}
              (apply shell/sh (first commands))]
          (if (or (= exit 0)
                  (not (seq err)))
            (recur (next commands))
            {::u/status :error
             ::u/message err}))))))


(def config-defaults
  {:version-page-url
   "https://documentation.uts.nlm.nih.gov/automating-downloads.html"

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
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))]
    (doto (u/*apply [::download-previous-snomed-ftr-version
                     ::get-latest-snomed-info!
                     ::write-snomed-snapshot-terminology-folder!
                     ::build-ftr-cfg
                     :ftr.core/apply-cfg
                     ::upload-to-gcp-bucket
                     ::generate-snomed-zen-package
                     ::push-zen-package]

                    cfg)
      (clojure.pprint/pprint))))
