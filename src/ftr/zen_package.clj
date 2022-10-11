(ns ftr.zen-package
  (:require [zen.core]
            [ftr.core]
            [ftr.utils.core]
            [clojure.string :as str]
            [zen.v2-validation]
            [clojure.java.io :as io]))


(defn build-ftr [ztx]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (map (partial zen.core/get-symbol ztx) syms)]
    (doseq [{:as vs, :keys [ftr]} value-sets]
      (when ftr
        (ftr.core/apply-cfg {:cfg ftr})))))


(defn expand-zen-packages [path]
  (let [modules (io/file path)]
    (when (and (.exists modules) (.isDirectory modules))
      (->> (.listFiles modules)
           (map (fn [x] (str x "/ftr")))
           (filter #(.isDirectory (io/file %)))))))


(defn expand-package-path [package-path]
  (let [ftr-path         (str package-path "/ftr")
        zen-packages-path (str package-path "/zen-packages")]
    (cond->> (expand-zen-packages zen-packages-path)
      (.exists (io/file ftr-path))
      (cons ftr-path))))


(defn ftr->memory-cache [ztx]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (map (partial zen.core/get-symbol ztx) syms)
        tags (-> (comp :tag :ftr)
                 (group-by value-sets)
                 keys)
        ftr-dirs (mapcat expand-package-path (:package-paths @ztx))
        ftr-dir (first ftr-dirs) ;;HACK TODO
        tag-index-paths (map (fn [tag]
                               {:tag tag
                                :path (format "%s/tags/%s.ndjson.gz" ftr-dir tag)}) tags)]
    (swap! ztx assoc :zen.fhir/ftr-cache
           (reduce (fn [acc {:keys [tag path]}]
                     (let [tag-index (ftr.utils.core/parse-ndjson-gz path)]
                       (assoc acc tag
                              (reduce (fn [acc {:as ti-entry
                                                :keys [hash name]}]
                                        (let [[_module-name vs-name]
                                              (str/split name #"\." 2)

                                              tf-path
                                              (format "%s/vs/%s/tf.%s.ndjson.gz"
                                                      ftr-dir
                                                      vs-name
                                                      hash)

                                              [cs vs & concepts]
                                              (ftr.utils.core/parse-ndjson-gz tf-path)]
                                          (assoc acc (:url vs) (map :code concepts))))
                                      {} tag-index))))
                   {} tag-index-paths))))


(defmethod zen.v2-validation/compile-key :zen.fhir/value-set
  [_ ztx vs]
  (let [{:as value-set,
         :keys [uri]
         {:keys [tag]} :ftr} (zen.core/get-symbol ztx (:symbol vs))
        ftr-cache (get @ztx :zen.fhir/ftr-cache)
        values* (set (get-in ftr-cache [tag uri]))]
    {:rule
     (fn [vtx data otps]
       (if-not (contains? values* data)
         (zen.v2-validation/add-err vtx :zen.fhir/value-set {:message (str "Expected '" data "' in " values*) :type ":zen.fhir/value-set"})
         vtx))}))
