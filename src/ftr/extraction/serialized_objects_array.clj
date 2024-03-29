(ns ftr.extraction.serialized-objects-array
  (:require [clojure.java.io :as io]
            [ftr.utils.unifn.core :as u]
            [ftr.utils.core]
            [clj-yaml.core]
            [cheshire.core]
            [clojure.pprint]))


(defmethod u/*fn ::create-value-set [cfg]
  {::result {:value-set
             (-> (:value-set cfg)
                 (assoc :resourceType "ValueSet")
                 (->> (merge {:status  "unknown"
                              :compose {:include [{:system (get-in cfg [:code-system :url])}]}})))}})


(defmethod u/*fn ::create-code-system [cfg]
  {::result {:code-system (-> (:code-system cfg)
                              (assoc :resourceType "CodeSystem")
                              (->> (merge {:status   "unknown"
                                           :content  "not-present"
                                           :valueSet (get-in cfg [:value-set :url])})
                                   (conj #{})))}})


(defn extract-mapped-value [mapping source]
  (update-vals mapping
               (fn [el]
                 (let [v (get-in source (:path el))]
                   (cond-> el
                     (not (nil? v))
                     (assoc :value v))))))


(defn extract-mapping [deserialized-object mapping]
  (update-vals mapping #(extract-mapped-value % deserialized-object)))

(ftr.utils.core/strip-nils {})

(defn format-concept-resource [extracted-mapping {:keys [system valueset]}]
  (let [code             (get-in extracted-mapping [:concept :code :value])
        display          (get-in extracted-mapping [:concept :display :value])
        status           (get-in extracted-mapping [:concept :deprecated? :value])
        deprecation-mark (get-in extracted-mapping [:concept :deprecated? :true-values])
        parent-id        (get-in extracted-mapping [:concept :parent-id :value])
        hierarchy-id     (get-in extracted-mapping [:concept :hierarchy-id :value])
        hierarchy        (get-in extracted-mapping [:concept :hierarchy :value])
        ancestors        (get-in extracted-mapping [:concept :ancestors :value])
        concept (ftr.utils.core/strip-nils
                  {:resourceType "Concept"
                   :system       system
                   :valueset     [valueset]
                   :code         code
                   :deprecated   (if (some #(= status %) deprecation-mark) true nil)
                   :display      display
                   :ancestors    ancestors
                   :parent-id    parent-id
                   :hierarchy-id hierarchy-id
                   :hierarchy    hierarchy
                   :property     (-> (:property extracted-mapping)
                                     (update-vals :value)
                                     ftr.utils.core/strip-nils
                                     not-empty)})]
    (when (:code concept)
      concept)))


(defn process-deserialized-objects [deserialized-objects
                                    {:as cfg, :keys [mapping]}]
  (->> deserialized-objects
       (map (fn process-deserialized-object [do]
              (-> do
                  (extract-mapping mapping)
                  (format-concept-resource cfg))))
       (filter identity)))


(defmethod u/*fn ::import [{:as cfg,
                            :keys [source-url format]}]
  (let [deserialized-objects
        (case format
          "yaml"   (clj-yaml.core/parse-stream (io/reader source-url))
          "ndjson" (->> source-url
                        (io/reader)
                        (line-seq)
                        (map (fn [line] (cheshire.core/parse-string line keyword)))))

        deserialized-objects-seq
        (process-deserialized-objects
          deserialized-objects
          (merge cfg
                 {:system   (get-in cfg [:code-system :url])
                  :valueset (get-in cfg [:value-set :url])
                  :mapping  (:mapping cfg)}))]
    {::result {:concepts deserialized-objects-seq}}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::create-value-set
                       ::create-code-system
                       ::import] cfg)))

(comment

  (clj-yaml.core/parse-stream (io/reader "/Users/ghrp/Downloads/external-care-team-relationship.yaml"))


  )
