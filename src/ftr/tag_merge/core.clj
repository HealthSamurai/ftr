(ns ftr.tag-merge.core
  (:require [ftr.utils.unifn.core :as u]
            [ftr.utils.core]
            [clojure.string :as str]
            [ftr.post-write-coordination.core]))


(defn get-vs-name-from-tag-index-entry [{:as _tag-index-entry,
                                         :keys [name]}]
  (second (str/split name #"\." 2)))


(defmethod u/*fn ::merge-tag [{:as ctx,
                               {:keys [tag]} :cfg
                               {:as _ftr-layout, :keys [vs-dir-path from-tag-index-path]} :ftr-layout
                               {{:keys [from vs-version-pins]} :merge} :cfg}]
  (let [from-tag-index (ftr.utils.core/parse-ndjson-gz from-tag-index-path)]
    (last
      (for [tag-index-entry
            from-tag-index]
        (let [vs-name
              (get-vs-name-from-tag-index-entry tag-index-entry)

              vs-path
              (str vs-dir-path \/ vs-name)

              from-vs-tag-file-path
              (format "%s/tag.%s.ndjson.gz" vs-path from)

              [{:as _tag-file-header, :keys [hash]} & _
               :as _from-vs-tag-file]
              (ftr.utils.core/parse-ndjson-gz from-vs-tag-file-path)

              vs-version-to-merge
              (if-let [pinned-version (get vs-version-pins vs-name)]
                pinned-version
                hash)]
          (u/*apply [:ftr.post-write-coordination.core/coordinate]
                    (-> ctx
                        (assoc-in [::merge-tag :hash] vs-version-to-merge)
                        (assoc-in [::merge-tag :value-set :url] vs-name)
                        (assoc-in [:ftr-layout :tf-tag-path] (format "%s/%s/tag.%s.ndjson.gz" vs-dir-path vs-name tag))
                        (assoc-in [:ftr-layout :vs-name-path] (format "%s/%s/" vs-dir-path vs-name)))))))))
