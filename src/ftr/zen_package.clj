(ns ftr.zen-package
  (:require [zen.core]
            [ftr.core]
            [ftr.utils.core]
            [clojure.string :as str]
            [zen.v2-validation]
            [clojure.java.io :as io]
            [clojure.pprint]))


(defn build-ftr [ztx]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (map (partial zen.core/get-symbol ztx) syms)
        ftr-configs (->> value-sets
                         (group-by :ftr)
                         keys
                         (filter identity))]
    (doseq [ftr ftr-configs]
      (ftr.core/apply-cfg {:cfg ftr}))))


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


(defn make-cache-by-tag-index [tag-index ftr-dir]
  (reduce (fn [{:as tag-cache, :keys [valuesets]}
               {:as _ti-entry
                :keys [hash name]}]
            (let [[_module-name vs-name]
                  (str/split name #"\." 2)

                  tf-path
                  (format "%s/vs/%s/tf.%s.ndjson.gz"
                          ftr-dir
                          vs-name
                          hash)

                  new-tf-reader
                  (ftr.utils.core/open-ndjson-gz-reader tf-path)

                  {codesystems "CodeSystem"
                   [{vs-url :url}] "ValueSet"}
                  (loop [line (.readLine new-tf-reader)
                         css&vs []]
                    (if (= (:resourceType line) "ValueSet")
                      (group-by :resourceType (conj css&vs line))
                      (recur (.readLine new-tf-reader) (conj css&vs line))))

                  codesystems-urls
                  (map :url codesystems)

                  tag-cache-with-updated-vss
                  (if (contains? valuesets vs-url)
                    (update-in tag-cache [:valuesets vs-url] into codesystems-urls)
                    (assoc-in tag-cache [:valuesets vs-url] (set codesystems-urls)))]
              (loop [{:as concept, :keys [code system display]}
                     (.readLine new-tf-reader)

                     {:as tag-cache, :keys [codesystems]}
                     tag-cache-with-updated-vss]

                (if-not (nil? concept)
                  (recur
                    (.readLine new-tf-reader)
                    (if (get-in codesystems [system code])
                      (update-in tag-cache [:codesystems system code :valueset] conj vs-url)
                      (assoc-in tag-cache [:codesystems system code] {:display display
                                                                      :valueset #{vs-url}})))
                  tag-cache))))
          {} tag-index))


(defn cache-by-tags [tag-index-paths]
  (reduce (fn [acc {:keys [tag path ftr-dir]}]
            (let [tag-index (ftr.utils.core/parse-ndjson-gz path)]
              (assoc acc tag (make-cache-by-tag-index tag-index ftr-dir))))
          {} tag-index-paths))


(defn ftr->memory-cache [ztx]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (map (partial zen.core/get-symbol ztx) syms)
        tags (-> (comp :tag :ftr)
                 (group-by value-sets)
                 keys)
        ftr-dirs (mapcat expand-package-path (:package-paths @ztx))
        tag-index-paths (mapcat (fn [tag]
                                  (map (fn [ftr-dir]
                                         {:tag tag
                                          :ftr-dir ftr-dir
                                          :path (format "%s/tags/%s.ndjson.gz" ftr-dir tag)})
                                       ftr-dirs))
                                tags)]
    (swap! ztx assoc :zen.fhir/ftr-cache (cache-by-tags tag-index-paths))))


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
