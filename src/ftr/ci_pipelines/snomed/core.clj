(ns ftr.ci-pipelines.snomed.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [net.cgrand.enlive-html :as html]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [ftr.core]
            [clojure.pprint])
  (:import java.io.File))


(defn get-parsed-html-by-url [url]
  (-> url
      (java.net.URL.)
      (html/html-resource)))


(defmethod u/*fn ::get-latest-snomed-info!
  ([{:as _ctx, :keys [version-page-url api-key]}]
   (let [parsed-html-page (get-parsed-html-by-url version-page-url)
         element-with-download-url (html/select parsed-html-page
                                                [[:p
                                                  (html/left [:h4
                                                              (html/has [[:a
                                                                          (html/attr= :name "download-the-us-edition-of-snomed-ct")]])])]])
         download-url (->> (html/select element-with-download-url
                                        [[html/text-node (html/left :b)]])
                           (filter #(str/includes? % "download"))
                           first)]
     {:snomed-info
      {:snomed-zip-url download-url
       :version (-> download-url
                    (str/split #"/")
                    (last)
                    (str/split #"_")
                    (last)
                    (str/split #"T")
                    (first))
       :complete-download-url (format "https://uts-ws.nlm.nih.gov/download?url=%s&apiKey=%s" download-url api-key)}})))


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
         :tag               version
         :source-type       :snomed
         :extractor-options {:db db
                             :code-system {:resourceType "CodeSystem"
                                           :id "snomedct"
                                           :url "http://snomed.info/sct"
                                           :description "SNOMEDCT US edition snapshot US1000124"
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
                                           :description "Includes all concepts from SNOMEDCT US edition snapshot US1000124. Both active and inactive concepts included, but is-a relationships only stored for active concepts"
                                           :url "http://snomed.info/sct"}}}})


(def config-defaults
  {:version-page-url
   "https://documentation.uts.nlm.nih.gov/automating-downloads.html"

   :db
   ftr.ci-pipelines.snomed.db/conn-str

   :ftr-path
   "/tmp/snomed-ftr"

   :extracted-snomed-out-dir
   "/tmp/extracted-snomed"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))]
    (clojure.pprint/pprint
      (u/*apply [::get-latest-snomed-info!
                 ::write-snomed-snapshot-terminology-folder!
                 ::build-ftr-cfg
                 :ftr.core/apply-cfg]
                cfg))))
