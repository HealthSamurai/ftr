(ns ftr.pull.core
  (:require [ftr.utils.core]
            [ftr.patch-generator.core]
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
                                tag-index
                                update-plan-file-name]
                :or {update-plan-file-name "update-plan"}}]
  (let [actual-tag-index-path (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag)
        actual-tag-index      (ftr.utils.core/parse-ndjson-gz actual-tag-index-path)
        client-tag-index-map  (tag-index->tag-index-map tag-index)
        actual-tag-index-map  (tag-index->tag-index-map actual-tag-index)
        tf-paths-to-migrate   (reduce-kv (fn [acc vs-name vs-hash]
                                           (let [client-tf-hash (get client-tag-index-map vs-name)]
                                             (if (and
                                                   (some? client-tf-hash)
                                                   (not= vs-hash client-tf-hash))
                                               (conj acc {:from    client-tf-hash
                                                          :to      vs-hash
                                                          :vs-name (second (str/split vs-name #"\." 2))})
                                               acc)))
                                       []
                                       actual-tag-index-map)
        update-plan-file-path (format "%s/%s.ndjson.gz" ftr-path update-plan-file-name)]
    (with-open [w (-> update-plan-file-path
                      (io/file)
                      (java.io.FileOutputStream.)
                      (java.util.zip.GZIPOutputStream. true)
                      (java.io.OutputStreamWriter.)
                      (java.io.BufferedWriter.))]
      (reduce (fn [acc {:keys [from to vs-name] :as tag-entry}]
                (let [->vs-path        (fn [suffix] (str (format "%s/%s/vs/%s/" ftr-path module vs-name) suffix))
                      tag-file-path    (->vs-path (format "tag.%s.ndjson.gz" tag))
                      tag-file         (ftr.utils.core/parse-ndjson-gz tag-file-path)
                      ?patch-file-path (first
                                         (keep (fn [m] (when (= m (dissoc tag-entry :vs-name))
                                                         (->vs-path (format "patch.%s.%s.ndjson.gz"
                                                                            from
                                                                            to))))
                                               (rest tag-file)))
                      old-tf           (->vs-path (format "tf.%s.ndjson.gz" from))
                      new-tf           (->vs-path (format "tf.%s.ndjson.gz" to))
                      patch-file       (or (when (and ?patch-file-path (-> ?patch-file-path io/file .exists))
                                             (rest (ftr.utils.core/parse-ndjson-gz ?patch-file-path)))
                                           (ftr.patch-generator.core/generate-patch! old-tf new-tf))
                      to-remove?       (comp #{"remove"} :op)
                      new-tf-reader    (ftr.utils.core/open-ndjson-gz-reader new-tf)]

                  ;; NOTE: read codesystem & valueset
                  (.write w (ftr.utils.core/generate-ndjson-row (.readLine new-tf-reader)))
                  (.write w (ftr.utils.core/generate-ndjson-row (.readLine new-tf-reader)))

                  ;; NOTE: collect resources to update
                  (doseq [resource (filter (comp not to-remove?) patch-file)]
                    (.write w (ftr.utils.core/generate-ndjson-row (dissoc resource :op))))

                  (->> patch-file
                       (filter to-remove?)
                       (map :id)
                       (update acc :remove-plan into))))
              {:remove-plan []
               :update-plan update-plan-file-path}
              tf-paths-to-migrate))))
