(ns ui.backend.operations
  (:import com.abahgat.suffixtree.GeneralizedSuffixTree
           com.abahgat.suffixtree.Utils)
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


(defn ndjson-gz-lines [path]
  (with-open [rdr (-> path
                      (io/input-stream)
                      (java.util.zip.GZIPInputStream.)
                      (io/reader))]
    (->> (line-seq rdr)
         doall)))



(defn deaccent [str]
  "Remove accent from string"
  ;; http://www.matt-reid.co.uk/blog_post.php?id=69
  (let [normalized (java.text.Normalizer/normalize str java.text.Normalizer$Form/NFD)]
    (clojure.string/replace normalized #"\p{InCombiningDiacriticalMarks}+" "")))


(defn sanitize-tree-key [str]
  (-> str
      deaccent
      com.abahgat.suffixtree.Utils/normalize))


(defn suffix-tree []
  {:index []
   :tree (com.abahgat.suffixtree.GeneralizedSuffixTree.)})


(defn put! [tree k v]
  (let [i         (count (:index tree))
        new-index (conj (:index tree) v)]
    (.put (:tree tree)
          (sanitize-tree-key k)
          i)
    {:index new-index
     :tree (:tree tree)}))


(defn put-multiple-keys! [tree ks v]
  (let [i         (count (:index tree))
        new-index (conj (:index tree) v)]
    (doseq [k ks]
      (when-not (str/blank? k)
        (.put (:tree tree)
              (sanitize-tree-key k)
              i)))
    {:index new-index
     :tree (:tree tree)}))


(defn search [tree s]
  (map (:index tree)
       (.search (:tree tree) s)))


(defmethod u/*fn ::ui [ctx]
  {:response {:status 200
              :body (hiccup.page/html5
                      [:head
                       [:link {:rel "stylesheet"
                               :date-name "vs/editor/editor.main"
                               :href "/static/monaco-editor/min/vs/editor/editor.main.css"}]
                       [:link {:href "/static/css/stylo.css"
                               :type "text/css"
                               :rel  "stylesheet"}]
                       [:link {:href "/static/css/main.css"
                               :type "text/css"
                               :rel  "stylesheet"}]]
                      [:body
                       [:div#root]
                       [:script "var require = { paths: { vs: '/static/monaco-editor/min/vs' } };"]
                       [:script {:src "/static/monaco-editor/min/vs/loader.js"}]
                       [:script {:src "/static/monaco-editor/min/vs/editor/editor.main.nls.js"}]
                       [:script {:src "/static/monaco-editor/min/vs/editor/editor.main.js"}]
                       [:script {:src (str "/static/js/frontend.js")}]])}})


(defn rpc-dispatch [ctx request method params]
  (keyword method))


(defmulti rpc #'rpc-dispatch)


(defmethod rpc :module-list [ctx request method params]
  {:status :ok
   :result (merge params
                  {:modules (->> (.listFiles (clojure.java.io/file "ftr"))
                                 (mapv #(keyword (.getName %))))})})


(defmethod rpc :module-tags [ctx request method {:as params :keys [module]}]
  (let [tags-dir-path  (str "ftr/" (name module) "/tags")
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
     :result (merge params {:tags tags})}))


(defmethod rpc :vs-list [ctx request method {:as params
                                             :keys [module tag]}]
  (let [tag-file-path    (str "ftr/" (name module) "/tags/" (name tag) ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        module-vs-names  (map :name tag-file-content)
        vs-names         (map #(second (clojure.string/split % #"\." 2))
                              module-vs-names)]
    {:status :ok
     :result (merge params {:vs-names vs-names})}))


(defmethod rpc :vs-expand [ctx request method {:as params
                                               :keys [module tag vs-name]}]
  (let [tag-file-path    (str "ftr/" (name module) "/vs/" (name vs-name) "/tag." (name tag) ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        current-hash     (:hash (first tag-file-content))
        tf-path          (str "ftr/" (name module) "/vs/" (name vs-name) "/tf." current-hash ".ndjson.gz")
        tf-file-content  (parse-ndjson-gz tf-path)
        [vs&cs concepts] (split-with
                           #(not= "Concept" (:resourceType %))
                           tf-file-content)

        {vss "ValueSet", css "CodeSystem"} (group-by :resourceType vs&cs)]
    {:status :ok
     :result (merge params
                    {:hash           current-hash
                     :value-sets     vss
                     :code-systems   css
                     :concepts       concepts
                     :concepts-count (count concepts)})}))


(defmethod rpc :current-hash [ctx request method {:as params :keys [module tag vs-name]}]
  (let [tag-file-path    (str "ftr/" (name module) "/vs/" (name vs-name) "/tag." (name tag) ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        current-hash     (:hash (first tag-file-content))]
    {:status :ok
     :result (merge params {:hash current-hash})}))


(defmethod rpc :vs-tag-hashes [ctx request method {:as params
                                                   :keys [module tag vs-name]}]
  (let [tag-file-path    (str "ftr/" (name module)
                              "/vs/" (name vs-name)
                              "/tag." (name tag)
                              ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        hashes           (->> (rest tag-file-content)
                              (mapcat (juxt :from :to))
                              dedupe)]
    {:status :ok
     :result (merge params {:hashes hashes})}))


(defmethod rpc :vs-expand-hash [ctx request method {:as params
                                                    :keys [module hash vs-name]}]
  (let [tf-path          (str "ftr/" (name module)
                              "/vs/" (name vs-name)
                              "/tf." hash ".ndjson.gz")
        tf-file-content  (parse-ndjson-gz tf-path)
        concepts         (drop-while
                           #(not= "Concept" (:resourceType %))
                           tf-file-content)
        [vs&cs concepts] (split-with
                           #(not= "Concept" (:resourceType %))
                           tf-file-content)

        {vss "ValueSet", css "CodeSystem"} (group-by :resourceType vs&cs)]
    {:status :ok
     :result (merge params
                    {:value-sets     vss
                     :code-systems   css
                     :concepts concepts
                     :concepts-count (count concepts)})}))


(defmethod rpc :vs-hashes [ctx request method {:as params :keys [module vs-name]}]
  (let [vs-dir-path (str "ftr/" (name module)
                         "/vs/" (name vs-name))

        vs-dir-file  (clojure.java.io/file vs-dir-path)
        vs-dir-files (.listFiles vs-dir-file)
        tfs          (filter #(clojure.string/starts-with? (.getName %) "tf.")
                             vs-dir-files)
        hashes       (map #(second (re-find #"tf\.(.*)\.ndjson\.gz"
                                            (.getName %)))
                          tfs)]
    {:status :ok
     :result (merge params {:hashes hashes})}))


(defmethod rpc :prev-hash [ctx request method {:as params
                                               :keys [module tag vs-name hash]}]
  (let [tag-file-path    (str "ftr/" (name module)
                              "/vs/" (name vs-name)
                              "/tag." (name tag)
                              ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        prev-hash        (->> (rest tag-file-content)
                              (filter #(= hash (:to %)))
                              first
                              :from)]
    {:status :ok
     :result (merge params {:hash prev-hash})}))


(defmethod rpc :next-hash [ctx request method {:as params
                                               :keys [module tag vs-name hash]}]
  (let [tag-file-path    (str "ftr/" (name module)
                              "/vs/" (name vs-name)
                              "/tag." (name tag)
                              ".ndjson.gz")
        tag-file-content (parse-ndjson-gz tag-file-path)
        next-hash        (->> (rest tag-file-content)
                              (filter #(= hash (:from %)))
                              first
                              :to)]
    {:status :ok
     :result (merge params {:hash next-hash})}))


(defmethod rpc :comp [ctx request method {:keys [methods params]}]
  (reduce (fn [acc method]
            (let [res (rpc ctx request method (:result acc))]
              (if (= :ok (:status res))
                res
                (reduced res))))
          {:result params}
          (reverse methods)))


(defn get-cache [cache-atom path build-fn]
  (-> cache-atom
      (swap! update-in path #(or % (build-fn)))
      (get-in path)))


(defn build-module-tag-vs-names-tree [ctx module tag]
  (let [vs-list-res (rpc ctx nil :vs-list {:module module :tag tag})
        vs-names    (get-in vs-list-res [:result :vs-names])]
    (reduce (fn [acc vs-name]
              (put! acc vs-name vs-name))
            (suffix-tree)
            vs-names)))


(defn get-module-tag-vs-names-tree [ctx module tag]
  (get-cache (:suffix-trees-cache ctx)
             [:vs-list (keyword module) (keyword tag)]
             #(build-module-tag-vs-names-tree ctx module tag)))


(defmethod rpc :search-in-vs-list [ctx request method {:as params
                                                       :keys [module tag search-str]}]
  (let [tree    (get-module-tag-vs-names-tree ctx module tag)
        matches (search tree (sanitize-tree-key search-str))]
    {:status :ok
     :result (merge params {:vs-names matches})}))


(defn build-module-hash-concepts-tree [ctx module vs-name hash]
  (let [vs-expand-res (rpc ctx nil :vs-expand-hash {:module  module
                                                    :vs-name vs-name
                                                    :hash    hash})
        vs-concepts   (get-in vs-expand-res [:result :concepts])]
    (reduce (fn [acc {:as concept :keys [code display system]}]
              (put-multiple-keys! acc [code display system] concept))
            (suffix-tree)
            vs-concepts)))


(defn get-module-hash-concepts-tree [ctx module vs-name hash]
  (get-cache (:suffix-trees-cache ctx)
             [:vs-expands (keyword module) (keyword vs-name) (keyword hash)]
             #(build-module-hash-concepts-tree ctx module vs-name hash)))


(defmethod rpc :search-in-hash-expand [ctx request method {:as params
                                                           :keys [module
                                                                  vs-name
                                                                  hash
                                                                  search-str]}]
  (let [tree    (get-module-hash-concepts-tree ctx module vs-name hash)
        matches (search tree (sanitize-tree-key search-str))]
    {:status :ok
     :result (merge params {:concepts matches
                            :concepts-count (count matches)})}))


(defmethod rpc :tf-content [ctx request method {:as params
                                                         :keys [module hash vs-name]}]
  (let [tf-path (str "ftr/" (name module) "/vs/" (name vs-name) "/tf." hash ".ndjson.gz")
        tf-file-content (ndjson-gz-lines tf-path)]
    {:status :ok
     :result (merge params {:tf-content tf-file-content})}))


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

  (rpc nil
       nil
       :vs-tag-hashes
       {:module  :hl7-fhir-us-core
        :tag     :init
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"})

  (rpc nil
       nil
       :vs-expand-hash
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"
        :hash    "1848162543321e2f9da9ca030bb71432e493de867d29828d0a527c84a95e7eeb"})

  (rpc nil
       nil
       :vs-hashes
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"})

  (rpc nil
       nil
       :current-hash
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"
        :tag     :init})

  (rpc nil
       nil
       :prev-hash
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"
        :tag     :init
        :hash    "95fa842ce62c521672d4ef406932fb0b1ccd33f551e47e1ee5237985e5a20591"})

  (rpc nil
       nil
       :next-hash
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"
        :tag     :init
        :hash    "1848162543321e2f9da9ca030bb71432e493de867d29828d0a527c84a95e7eeb"})

  (rpc nil
       nil
       :comp
       {:methods [:vs-expand-hash :prev-hash :current-hash]
        :params {:module  :hl7-fhir-us-core
                 :tag     :init
                 :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"}})

  (def suffix-trees-cache (atom {}))

  (def ctx {:suffix-trees-cache suffix-trees-cache})

  (rpc ctx
       nil
       :search-in-vs-list
       {:module  :hl7-fhir-us-core
        :tag     :init
        :search-str "se"})

  (rpc ctx
       nil
       :search-in-vs-list
       {:module  :hl7-fhir-us-core
        :tag     :init
        :search-str "ethnicity"})

  (rpc ctx
       nil
       :current-hash
       {:module  :hl7-fhir-us-core
        :tag     :init
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-detailed-ethnicity"})

  (rpc ctx
       nil
       :search-in-hash-expand
       {:module  :hl7-fhir-us-core
        :tag     :init
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-detailed-ethnicity"
        :hash "8e9733d0edbe56ca3f3efefbaedb6882cf9437b4973df8cb945c5a75b825cce2"
        :search-str "un"})

  (rpc nil
       nil
       :tf-content
       {:module  :hl7-fhir-us-core
        :vs-name "http:--hl7.org-fhir-us-core-ValueSet-us-core-sexual-orientation"
        :hash    "1848162543321e2f9da9ca030bb71432e493de867d29828d0a527c84a95e7eeb"})

  nil)


(defmethod u/*fn ::rpc [{:as ctx, :keys [request]}]
  (let [{:keys [method params]} (:body request)]
    {:response {:status  200
                :headers {"Content-Type" "application/json"}
                :body    (rpc ctx request method params)}}))
