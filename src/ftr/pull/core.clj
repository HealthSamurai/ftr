(ns ftr.pull.core
  (:require [ftr.utils.core]
            [clojure.java.io :as io]
            [clojure.string :as str]))


(defn init [{:as _cfg, :keys [tag ftr-path module]}
            & [{:as params, :keys [patch-plan-file-name]
                :or {patch-plan-file-name (ftr.utils.core/gen-uuid)}}]]
  (let [tag-index-path (format "%s/%s/tags/%s.ndjson.gz"
                               ftr-path
                               module
                               tag)
        tag-index (ftr.utils.core/parse-ndjson-gz tag-index-path)
        temp-patch-plan-file (format "%s/%s.ndjson.gz"
                                     ftr-path
                                     patch-plan-file-name)]
    (with-open [w (-> temp-patch-plan-file
                      (io/file)
                      (java.io.FileOutputStream.)
                      (java.util.zip.GZIPOutputStream. true)
                      (java.io.OutputStreamWriter.)
                      (java.io.BufferedWriter.))]
      (doseq [{:as _ti-entry, :keys [hash name]} tag-index
              :let [trimmed-name (second (str/split name #"\." 2))]]
        (let [tf-path (format "%s/%s/vs/%s/tf.%s.ndjson.gz"
                              ftr-path
                              module
                              trimmed-name
                              hash)]
          (with-open [r (-> tf-path
                            (io/input-stream)
                            (java.util.zip.GZIPInputStream.)
                            (io/reader))]
            (doseq [l (line-seq r)]
              (.write w (str l \newline)))))))))


(defn tag-index->tag-index-map [tag-index]
  (reduce (fn [acc {:as tag-index-entry, :keys [hash name]}]
            (assoc acc name hash)) {} tag-index))


(defn migrate [{:as cfg, :keys [ftr-path
                                module
                                tag
                                tag-index]}]
  (let [actual-tag-index-path (format "%s/%s/tags/%s" ftr-path module tag)
        actual-tag-index (ftr.utils.core/parse-ndjson-gz actual-tag-index-path)
        client-tag-index-map (tag-index->tag-index-map tag-index)
        actual-tag-index-map (tag-index->tag-index-map actual-tag-index)
        tf-paths-to-migrate (reduce-kv (fn [acc k v]
                                         (let [client-tf-hash (get k client-tag-index-map)]
                                           (if (not= v client-tf-hash)
                                            (conj acc {:from client-tf-hash
                                                       :to v})
                                            acc))
                                         )  [] actual-tag-index-map)])

  )
