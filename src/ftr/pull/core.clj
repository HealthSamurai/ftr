(ns ftr.pull.core
  (:require [ftr.utils.core]
            [ftr.patch-generator.core]
            [clojure.java.io :as io]
            [clojure.string :as str]))


(defn init [{:as _cfg, :keys [tag ftr-path module]}
            & [{:as params, :keys [patch-plan-file-name]
                :or {patch-plan-file-name (str "update-plan-" (ftr.utils.core/gen-uuid))}}]]
  (let [tag-index-path (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag)
        tag-index (ftr.utils.core/parse-ndjson-gz tag-index-path)
        patch-plan-file-path (format "%s/%s.ndjson.gz"
                                     ftr-path
                                     patch-plan-file-name)]
    (with-open [w (ftr.utils.core/open-gz-writer patch-plan-file-path)]
      (doseq [{:as _ti-entry, :keys [hash name]} tag-index
              :let [trimmed-name (second (str/split name #"\." 2))]]
        (let [tf-path (format "%s/%s/vs/%s/tf.%s.ndjson.gz"
                              ftr-path
                              module
                              trimmed-name
                              hash)]
          (with-open [r (ftr.utils.core/open-gz-reader tf-path)]
            (doseq [l (line-seq r)]
              (.write w (str l \newline)))))))
    {:patch-plan patch-plan-file-path}))


(defn extract-vs-name [name]
  (second (str/split name #"\." 2)))


(defn tag-index->tag-index-map [tag-index]
  (reduce (fn [acc {:as tag-index-entry, :keys [hash name]}]
            (assoc acc name hash)) {} tag-index))


(defn diff-tag-indexes [a b]
  (let [all-keys (into #{} (concat (keys a) (keys b)))]
    (reduce (fn [acc k]
              (let [a-entry (get a k)
                    b-entry (get b k)]
                (cond
                  (nil? a-entry)
                  (conj acc {:state :add
                             :vs-name k
                             :from a-entry
                             :to b-entry})

                  (nil? b-entry)
                  (conj acc {:state :remove
                             :vs-name k
                             :from a-entry
                             :to b-entry})

                  (not= (get a k) (get b k))
                  (conj acc {:state :updated
                             :vs-name k
                             :from a-entry
                             :to b-entry}))))
            [] all-keys)))


(defn migrate [{:as _cfg, :keys [ftr-path module tag tag-index patch-plan-file-name]
                :or {patch-plan-file-name "update-plan"}}]
  (let [actual-tag-index-path (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag)
        actual-tag-index      (ftr.utils.core/parse-ndjson-gz actual-tag-index-path)
        client-tag-index-map  (tag-index->tag-index-map tag-index)
        actual-tag-index-map  (tag-index->tag-index-map actual-tag-index)
        tag-indexes-diff      (diff-tag-indexes client-tag-index-map actual-tag-index-map)
        patch-plan-file-path (format "%s/%s.ndjson.gz" ftr-path patch-plan-file-name)]
    (with-open [w (ftr.utils.core/open-gz-writer patch-plan-file-path)]
      (reduce (fn [acc {:keys [state from to vs-name] :as tag-entry}]
                (let [vs-name (extract-vs-name vs-name)
                      ->vs-path (fn [suffix] (str (format "%s/%s/vs/%s/" ftr-path module vs-name) suffix))
                      ?old-tf           (when from (->vs-path (format "tf.%s.ndjson.gz" from)))
                      ?new-tf           (when to (->vs-path (format "tf.%s.ndjson.gz" to)))]
                  (condp = state
                    :remove
                    (->>
                      (ftr.utils.core/parse-ndjson-gz ?old-tf)
                      (map :id)
                      (update acc :remove-plan into))

                    :add
                    (do (doseq [l (line-seq (ftr.utils.core/open-gz-reader ?new-tf))]
                          (.write w (str l \newline)))
                        acc)

                    :updated
                    (let [tag-file-path (->vs-path (format "tag.%s.ndjson.gz" tag))
                          tag-file (ftr.utils.core/parse-ndjson-gz tag-file-path)
                          ?patch-file-path (first
                                             (keep (fn [m] (when (= m (dissoc tag-entry :vs-name))
                                                             (->vs-path (format "patch.%s.%s.ndjson.gz"
                                                                                from
                                                                                to))))
                                                   (rest tag-file)))
                          patch (or (when (and ?patch-file-path (-> ?patch-file-path io/file .exists))
                                      (rest (ftr.utils.core/parse-ndjson-gz ?patch-file-path)))
                                    (ftr.patch-generator.core/generate-patch! ?old-tf ?new-tf))
                          new-tf-reader (ftr.utils.core/open-ndjson-gz-reader ?new-tf)
                          codesystems&value-set (loop [line (.readLine new-tf-reader)
                                                       acc  []]
                                                  (if (= (:resourceType line) "ValueSet")
                                                    (conj acc line)
                                                    (recur (.readLine new-tf-reader) (conj acc line))))
                          {:strs [add remove update]} (group-by :op patch)
                          concepts (->> (into add update)
                                        (map #(dissoc % :op :_source)))
                          ids-to-remove (map :id remove)]

                      (doseq [l (into codesystems&value-set concepts)]
                        (.write w (ftr.utils.core/generate-ndjson-row l)))

                      (clojure.core/update acc :remove-plan into ids-to-remove)))))
              {:remove-plan []
               :patch-plan patch-plan-file-path}
              tag-indexes-diff))))
