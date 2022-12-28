(ns ui.backend.operations
  (:require [ftr.utils.unifn.core :as u]
            [cheshire.core]
            [hiccup.core]
            [hiccup.page]
            [stylo.core]
            [org.httpkit.server :as server]
            [clojure.java.io :as io]
            [clojure.walk]
            [clojure.string :as str]))


(defn parse-ndjson-gz [path]
  (with-open [rdr (-> path
                      (io/input-stream)
                      (java.util.zip.GZIPInputStream.)
                      (io/reader))]
    (->> (line-seq rdr)
         (mapv (fn [json-row]
                 (cheshire.core/parse-string json-row keyword))))))


(defmethod u/*fn ::root [ctx]
  {:response {:status 200
              :body (hiccup.page/html5
                     [:head
                      [:link {:href "/static/css/stylo.css"
                              :type "text/css"
                              :rel  "stylesheet"}]
                      [:link {:href "/static/css/main.css"
                              :type "text/css"
                              :rel  "stylesheet"}]]
                     [:body
                      [:div#root]
                      [:script {:src (str "/static/js/frontend.js")}]])}})


(defn rpc-dispatch [ctx request method params]
  (keyword method))


(defmulti rpc #'rpc-dispatch)


(defmethod rpc :module-list [ctx request method params]
  {:status :ok
   :result {:modules (->> (.listFiles (clojure.java.io/file "ftr"))
                          (mapv #(keyword (.getName %))))}})


(defmethod rpc :module-tags [ctx request method params]
  (let [tags-dir-path  (str "ftr/"
                            (name (:module params))
                            "/tags")
        tags-dir-file  (clojure.java.io/file tags-dir-path)
        tags-dir-files (.listFiles tags-dir-file)

        tags-ndjson-files (filter #(clojure.string/ends-with? (.getName %)
                                                              ".ndjson.gz")
                                  tags-dir-files)

        tags (map #(-> (.getName %)
                       (clojure.string/replace #".ndjson.gz$" "")
                       keyword)
                  tags-ndjson-files)]
    {:status :ok
     :result {:tags tags}}))


(defmethod rpc :vs-list [ctx request method {:keys [module tag]}]
  (let [tag-file-path    (str "ftr/" (name module) "/tags/" (name tag) ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        module-vs-names  (map :name tag-file-content)
        vs-names         (map #(second (clojure.string/split % #"\." 2))
                              module-vs-names)]
    {:status :ok
     :result vs-names}))


(defmethod rpc :vs-expand [ctx request method {:keys [module tag vs-name]}]
  (let [tag-file-path    (str "ftr/" (name module) "/vs/" (name vs-name) "/tag." (name tag) ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        current-hash     (:hash (first tag-file-content))
        tf-path          (str "ftr/" (name module) "/vs/" (name vs-name) "/tf." current-hash ".ndjson.gz")
        tf-file-content  (parse-ndjson-gz tf-path)
        concepts         (drop-while
                           #(not= "Concept" (:resourceType %))
                           tf-file-content)]
    {:status :ok
     :result {:concepts concepts}}))


(comment
  (rpc nil
       nil
       :module-list
       nil)

  (rpc nil
       nil
       :module-tags
       {:module :hl7-fhir-us-core})

  (rpc nil
       nil
       :vs-list
       {:module :hl7-fhir-us-core
        :tag :init})

  (rpc nil
       nil
       :vs-expand
       {:module  :hl7-fhir-us-core
        :tag     :init
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"})



  nil)


(defmethod u/*fn ::rpc [{:as ctx, :keys [request]}]
  (let [{:keys [method params]} (:body request)]
    {:response {:status  200
                :headers {"Content-Type" "application/json"}
                :body    (rpc ctx request method params)}}))
