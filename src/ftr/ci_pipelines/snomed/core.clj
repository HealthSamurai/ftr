(ns ftr.ci-pipelines.snomed.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [net.cgrand.enlive-html :as enlive-html]))


(def SNOMED-API-KEY "")


(def config
  {:version-page-url
   "https://documentation.uts.nlm.nih.gov/automating-downloads.html"

   :api-key
   SNOMED-API-KEY})


(defn get-parsed-html-by-url [url]
  (-> url
      (java.net.URL.)
      (enlive-html/html-resource)))


(defn get-latest-snomed-download-url
  ([] (get-latest-snomed-download-url config))
  ([{:as _cfg, :keys [version-page-url api-key]}]
   (let [parsed-html-page (get-parsed-html-by-url version-page-url)
         element-with-download-url (enlive-html/select parsed-html-page
                                                       [[:p
                                                         (html/left [:h4
                                                                     (html/has [[:a
                                                                                 (html/attr= :name "download-the-us-edition-of-snomed-ct")]])])]])
         download-url (->> (enlive-html/select element-with-download-url
                                               [[enlive-html/text-node (html/left :b)]])
                           (filter #(str/includes? % "download"))
                           first)]
     (format "https://uts-ws.nlm.nih.gov/download?url=%s&apiKey=%s" download-url api-key))))


(comment
  (def latest-url (get-latest-snomed-download-url))

  (-> latest-url
      (io/input-stream)
      (java.util.zip.ZipInputStream.))

  nil)
