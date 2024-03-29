(ns ftr.pull.core
  (:require [ftr.utils.core]
            [ftr.patch-generator.core]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [klog.core :as klog]))


(defn init [{:as _cfg, :keys [tag ftr-path module]}
            & [{:as _params, :keys [patch-plan-file-name]
                :or {patch-plan-file-name (str "update-plan-" (ftr.utils.core/gen-uuid))}}]]
  (let [tag-index-path (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag)
        tag-index (ftr.utils.core/parse-ndjson-gz tag-index-path)
        bulk-patch-plans-folder (doto "/tmp/ftr-bulk-patch-plans" (-> (io/file) (.mkdirs)))
        patch-plan-file-path (format "%s/%s.ndjson.gz"
                                     bulk-patch-plans-folder
                                     patch-plan-file-name)]
    (let [counter (atom 0)
          tag-index-size (count tag-index)]
      (with-open [w (ftr.utils.core/open-gz-writer patch-plan-file-path)]
        (doseq [{:as _ti-entry, :keys [hash name]} tag-index
                :let [trimmed-name (second (str/split name #"\." 2))]]
          (let [tf-path (format "%s/%s/vs/%s/tf.%s.ndjson.gz"
                                ftr-path
                                module
                                trimmed-name
                                hash)]
          (swap! counter inc)
          (klog/info :ftr/initial-pull
                     {:msg (format "Downloading %s terminology file %s/%s"
                                   tf-path
                                   @counter
                                   tag-index-size)})
            (with-open [r (ftr.utils.core/open-gz-reader tf-path)]
              (doseq [l (line-seq r)]
                (.write w (str l \newline))))))))
    {:patch-plan patch-plan-file-path}))


(defn extract-vs-name [name]
  (second (str/split name #"\." 2)))


(defn tag-index->tag-index-map [tag-index]
  (reduce (fn [acc {:as _tag-index-entry, :keys [hash name]}]
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


(defn infer-patch-chain [{:as _cfg,
                          :keys [tf-tag-path from-hash to-hash]}]
  (let [[_header & from&to-entries
         :as _parsed-tf-tag-file]
        (ftr.utils.core/parse-ndjson-gz tf-tag-path)]
    (loop [from from-hash
           patch-chain []]
      (let [{:as from&to-entry,
             :keys [_from to]} (first (filter #(= from (:from %)) from&to-entries))]
        (cond
          (= to to-hash)
          {:patch-chain (conj patch-chain from&to-entry)
           :finalized? true}

          from&to-entry
          (recur to (conj patch-chain from&to-entry))

          (nil? from&to-entry)
          {:patch-chain patch-chain
           :finalized? false})))))


(defn generate-intermediate-patch-plan [{:as cfg, :keys [vs-path old-tf new-tf]}]
  (let [{:keys [patch-chain]} (infer-patch-chain cfg)
        patches-paths (map #(format "%s/patch.%s.%s.ndjson.gz" vs-path (:from %) (:to %)) patch-chain)]
    (if (seq patch-chain)
      (reduce (fn [acc patch-path]
                (let [[_header & patch-entries] (ftr.utils.core/parse-ndjson-gz patch-path)]
                  (reduce (fn [acc {:as patch-entry, :keys [op id]}]
                            (case op
                              "add"
                              (assoc-in acc [:patch-plan id] patch-entry)

                              "update"
                              (assoc-in acc [:patch-plan id] patch-entry)

                              "remove"
                              (-> acc
                                  (update :remove-plan conj id)
                                  (update :patch-plan dissoc id))))
                          acc patch-entries)))
              {:patch-plan {}
               :remove-plan #{}}
              patches-paths)
      (let [{:strs [add remove update]} (group-by :op (ftr.patch-generator.core/generate-patch! old-tf new-tf))]
           {:patch-plan (reduce (fn [acc c] (assoc acc (:id c) c)) {} (into add update))
            :remove-plan (map :id remove)}))))


(defn migrate [{:as cfg, :keys [ftr-path module tag tag-index tag-index-hash patch-plan-file-name]
                :or {patch-plan-file-name "update-plan"}}]
  (when-not (= tag-index-hash
               (slurp (format "%s/%s/tags/%s.hash" ftr-path module tag)))
      (let [actual-tag-index-path (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag)
         actual-tag-index      (ftr.utils.core/parse-ndjson-gz actual-tag-index-path)
         client-tag-index-map  (tag-index->tag-index-map tag-index)
         actual-tag-index-map  (tag-index->tag-index-map actual-tag-index)
         tag-indexes-diff      (diff-tag-indexes client-tag-index-map actual-tag-index-map)
         bulk-patch-plans-folder (doto "/tmp/ftr-bulk-patch-plans" (-> (io/file) (.mkdirs)))
         patch-plan-file-path (format "%s/%s.ndjson.gz" bulk-patch-plans-folder patch-plan-file-name)]
     (with-open [w (ftr.utils.core/open-gz-writer patch-plan-file-path)]
       (reduce (fn [acc {:keys [state from to vs-name] :as _tag-entry}]
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
                           new-tf-reader (ftr.utils.core/open-ndjson-gz-reader ?new-tf)
                           codesystems&value-set (loop [line (.readLine new-tf-reader)
                                                        acc  []]
                                                   (if (= (:resourceType line) "ValueSet")
                                                     (conj acc line)
                                                     (recur (.readLine new-tf-reader) (conj acc line))))

                           {:as _intermediate-patch-plan
                            :keys [patch-plan remove-plan]}
                           (generate-intermediate-patch-plan (assoc cfg
                                                                    :vs-path (format "%s/%s/vs/%s/" ftr-path module vs-name)
                                                                    :tf-tag-path tag-file-path
                                                                    :from-hash from
                                                                    :to-hash to
                                                                    :old-tf ?old-tf
                                                                    :new-tf ?new-tf))
                           concepts (->> (vals patch-plan)
                                         (map #(dissoc % :op :_source)))]

                       (doseq [l (into codesystems&value-set concepts)]
                         (.write w (ftr.utils.core/generate-ndjson-row l)))

                       (clojure.core/update acc :remove-plan into remove-plan)))))
               {:remove-plan []
                :patch-plan patch-plan-file-path}
               tag-indexes-diff)))))
