(ns ftr.extraction.ftr
  (:require [ftr.utils.unifn.core :as u]
            [ftr.utils.core]
            [zen.core]
            [ftr.extraction.ig.value-set-expand]
            [ftr.extraction.ig.core]
            ))


(defn prepare-concept [{:as concept,
                        :keys [ancestors
                               hierarchy]}]
  (let [parents (->> (if ancestors (keys ancestors) hierarchy)
                     (map keyword)
                     (set))]
    (-> concept
        (assoc :zen.fhir/code-kw (keyword (:code concept))
               :zen.fhir/system-kw (keyword (:system concept))
               :zen.fhir/parents parents
               :zen.fhir/resource concept))))


(defn prepare-concepts [concepts]
  (->> concepts
       (map prepare-concept)
       (reduce (fn [acc {:as concept, :keys [system code]}]
                 (assoc-in acc [system code] concept))
               {})))


(defn prepare-valueset [valueset]
  (assoc valueset :zen.fhir/resource valueset))


(defn prepare-valuesets [valuesets]
  (->> valuesets
       (map prepare-valueset)
       (reduce (fn [acc {:as vs, :keys [url]}]
                 (assoc acc url vs)) {})))


(defn tf-to-fhir-inter [tf-path]
  (let [tf-content (ftr.utils.core/parse-ndjson-gz tf-path)
        [_codesystems valuesets concepts] (partition-by :resourceType tf-content)

        prepared-concepts (prepare-concepts concepts)
        prepared-valuesets (prepare-valuesets valuesets)]
    {"Concept" prepared-concepts
     "ValueSet" prepared-valuesets}))


(defn ftr->fhir-inter [tf-paths]
  {:fhir/inter (->> tf-paths
                    (map tf-to-fhir-inter)
                    (reduce ftr.utils.core/deep-merge {}))})


(defn construct-tag-index-info [src-url tag]
  {:src-url src-url
   :path    (format "%s/tags/%s.ndjson.gz"
                 src-url
                 tag)})


(defmethod u/*fn ::load-terminology [{:as _ctx, :keys [target-tag
                                                       source-url
                                                       target-tags
                                                       value-set]}]
  (let [tag-index-paths (if (and target-tag source-url)
                          [(construct-tag-index-info source-url target-tag)]
                          (map (fn [[url tag]] (construct-tag-index-info url tag))
                               target-tags))
        tag-indexes (map (fn [{:keys [path src-url]}]
                           (->> path
                                (ftr.utils.core/parse-ndjson-gz)
                                (map #(assoc % :src-url src-url))))
                         tag-index-paths)
        tf-paths (map (fn [{:as   _tag-index-row,
                            :keys [hash name src-url]}]
                           (let [vs-name (ftr.utils.core/separate-vs&module-names name)
                                 tf-path (format "%s/vs/%s/tf.%s.ndjson.gz"
                                                 src-url vs-name hash)]
                          tf-path))
                      (flatten tag-indexes))]
    {:ztx (zen.core/new-context (-> (ftr->fhir-inter tf-paths)
                                    (assoc-in [:fhir/inter "ValueSet" (:url value-set)] (prepare-valueset value-set))))}))


(defmethod u/*fn ::expand-valuesets [{:as _ctx, :keys [ztx]}]
  (ftr.extraction.ig.value-set-expand/denormalize-value-sets-into-concepts ztx)
  (swap! ztx update-in [:fhir/inter "Concept"]
         (fn [codesystems-concepts]
           (update-vals (ftr.extraction.ig.core/flatten-nested-map
                          codesystems-concepts
                          2
                          (fn [[system code]] (str system \- code)))
                        #(ftr.extraction.ig.core/process-concept ztx %))))
  {})



(defn import-from-cfg [{:as cfg, :keys [value-set]}]
  (-> [::load-terminology
       ::expand-valuesets
       :ftr.extraction.ig.core/compose-tfs]
      (u/*apply cfg)
      :ftr.extraction.ig.core/result
      (get (:url value-set))))
