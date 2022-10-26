(ns ftr.ingestion-coordinator.core
  (:require [ftr.utils.unifn.core :as u]
            [clojure.java.io :as io]
            [ftr.utils.core]
            [ftr.patch-generator.core]))


(defn update-tf-tag! [tf-tag-path new-sha256]
  (let [tf-tag (ftr.utils.core/parse-ndjson-gz tf-tag-path)
        current-sha256 (get-in tf-tag [0 :hash])]
    (when (not= current-sha256 new-sha256)
      (->> (-> tf-tag
               (update 0 assoc :hash new-sha256)
               (conj {:from current-sha256 :to new-sha256}))
           (ftr.utils.core/spit-ndjson-gz! tf-tag-path))
      {:ingestion-coordinator {:generate-patch? true
                               :old-sha256 current-sha256}})))


(defn create-tf-tag! [tf-tag-path tag sha256
                      & [{:keys [move-tag? old-tag old-tf-tag-path]}]]
  (if (and move-tag? (ftr.utils.core/file-exists? old-tf-tag-path))
    (let [[{:as _tag-file-entry, :keys [hash]}]
          (ftr.utils.core/parse-ndjson-gz old-tf-tag-path)]
      (ftr.utils.core/spit-ndjson-gz! tf-tag-path
                                      [{:tag tag :hash sha256 :from-tag old-tag}
                                       {:from hash :to sha256}])
      {:ingestion-coordinator {:generate-patch? true
                               :old-sha256 hash}})
    (ftr.utils.core/spit-ndjson-gz! tf-tag-path [{:tag tag :hash sha256}])))


(defmethod u/*fn ::tf-tag-upsert [{:as _ctx,
                                   {:as cfg, :keys [tag] {:keys [new-tag old-tag]} :move-tag} :cfg
                                   {:keys [tf-sha256]} :write-result
                                   {:keys [old-tf-tag-path tf-tag-path]} :ftr-layout}]
  (if new-tag
    (create-tf-tag! tf-tag-path new-tag tf-sha256 {:move-tag? true
                                                   :old-tag old-tag
                                                   :old-tf-tag-path old-tf-tag-path})
    (if (ftr.utils.core/file-exists? tf-tag-path)
      (update-tf-tag! tf-tag-path tf-sha256)
      (create-tf-tag! tf-tag-path tag tf-sha256))))


(defn update-tag-index! [tag-index-path vs-name module new-sha256]
  (let [tag-index (ftr.utils.core/parse-ndjson-gz tag-index-path)
        {:as tag-index-entry, :keys [idx entry]}
        (keep-indexed (fn [idx {:as v, :keys [name]}]
                        (when (= name (format "%s.%s" module vs-name))
                          {:idx idx
                           :entry v}))
                      tag-index)]
    (->> (if (seq tag-index-entry)
           (update tag-index idx assoc :hash new-sha256)
           (conj tag-index {:name (format "%s.%s" module vs-name) :hash new-sha256}))
         (ftr.utils.core/spit-ndjson-gz! tag-index-path))))


(defn create-tag-index! [tag-index-path vs-name module sha256]
  (->>
    [{:name (format "%s.%s" module vs-name) :hash sha256}]
    (ftr.utils.core/spit-ndjson-gz! tag-index-path)))


(defmethod u/*fn ::tag-index-upsert [{:as _ctx, ;;TODO
                                      {:keys [module move-tag]} :cfg
                                      {:keys [tf-sha256 value-set]} :write-result
                                      {:keys [tag-index-path
                                              new-tag-index-path]} :ftr-layout}]
  (if new-tag-index-path
    (if (ftr.utils.core/file-exists? new-tag-index-path)
      (update-tag-index! new-tag-index-path (ftr.utils.core/escape-url (:url value-set)) module tf-sha256)
      (create-tag-index! new-tag-index-path (ftr.utils.core/escape-url (:url value-set)) module tf-sha256))
    (if (ftr.utils.core/file-exists? tag-index-path)
      (update-tag-index! tag-index-path (ftr.utils.core/escape-url (:url value-set)) module tf-sha256)
      (create-tag-index! tag-index-path (ftr.utils.core/escape-url (:url value-set)) module tf-sha256))))


(defn generate-tf-name [sha]
  (format "tf.%s.ndjson.gz" sha))


(defmethod u/*fn ::move-terminology-file [{:as _ctx,
                                           {:keys [tf-path vs-name-path]} :ftr-layout
                                           {:keys [terminology-file tf-sha256]} :write-result
                                           {:keys [generate-patch? old-sha256]} :ingestion-coordinator}]
  (let [old-tf-path (format "%s/%s" vs-name-path (generate-tf-name old-sha256))]
    (cond-> {:ingestion-coordinator {:tf-file (ftr.utils.core/move-file! terminology-file tf-path)}}
      generate-patch?
      (->
        (assoc-in [:ftr-layout :old-tf-path] old-tf-path)
        (assoc-in [:ftr-layout :tf-patch-path] (format "%s/patch.%s.%s.ndjson.gz" vs-name-path old-sha256 tf-sha256))
        (assoc-in [:ingestion-coordinator :old-tf-file] (io/file old-tf-path))))))


(defmethod u/*fn ::coordinate-tf-ingestion [ctx]
  (u/*apply [::tf-tag-upsert
             ::tag-index-upsert
             ::move-terminology-file
             :ftr.patch-generator.core/generate-patch] ctx))


(defmethod u/*fn ::ingest-terminology-file
  [{:as ctx,
    {:keys [terminology-file]} :write-result
    {:keys [tf-path]} :ftr-layout}]
  (u/*apply ::coordinate-tf-ingestion ctx))
