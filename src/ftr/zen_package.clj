(ns ftr.zen-package
  (:require [zen.core]
            [ftr.core]
            [ftr.utils.core]
            [clojure.string :as str]
            [zen.v2-validation]
            [clojure.java.io :as io]
            [clojure.pprint]
            [zen.cli])
  (:import (java.util UUID)))


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
           (map (fn [x] (str x "/ftr/")))
           (filter #(.isDirectory (io/file %)))))))


(defn expand-package-path [package-path]
  (let [ftr-path         (str package-path "/ftr")
        zen-packages-path (str package-path "/zen-packages")]
    (cond->> (expand-zen-packages zen-packages-path)
      (.exists (io/file ftr-path))
      (cons ftr-path))))


(defn make-ftr-index-by-tag-index [tag-index ftr-dir module]
  (reduce (fn [{:as ftr-index-by-tag, :keys [valuesets]}
               {:as _ti-entry
                :keys [hash name]}]
            (let [[_module-name vs-name]
                  (str/split name #"\." 2)

                  tf-path
                  (format "%s/%s/vs/%s/tf.%s.ndjson.gz"
                          ftr-dir
                          module
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

                  ftr-index-by-tag-with-updated-vss
                  (if (contains? valuesets vs-url)
                    (update-in ftr-index-by-tag [:valuesets vs-url] into codesystems-urls)
                    (assoc-in ftr-index-by-tag [:valuesets vs-url] (set codesystems-urls)))]
              (loop [{:as concept, :keys [code system display]}
                     (.readLine new-tf-reader)

                     {:as ftr-index-by-tag, :keys [codesystems]}
                     ftr-index-by-tag-with-updated-vss]

                (if-not (nil? concept)
                  (recur
                    (.readLine new-tf-reader)
                    (if (get-in codesystems [system code])
                      (update-in ftr-index-by-tag [:codesystems system code :valueset] conj vs-url)
                      (assoc-in ftr-index-by-tag [:codesystems system code] {:display display
                                                                             :valueset #{vs-url}})))
                  ftr-index-by-tag))))
          {} tag-index))


(defn index-by-tags [tag-index-paths]
  (reduce (fn [acc {:keys [tag path ftr-dir module]}]
            (let [tag-index (ftr.utils.core/parse-ndjson-gz path)]
              (update acc tag ftr.utils.core/deep-merge (make-ftr-index-by-tag-index tag-index ftr-dir module))))
          {} tag-index-paths))


(defn enrich-vs [vs]
  (let [zen-file (:zen/file vs)
        path-to-package (->> (str/split zen-file #"/")
                             (take-while (complement #{"zrc"})))
        zen-package-name (last path-to-package)
        inferred-ftr-dir (-> path-to-package
                             vec
                             (conj "ftr")
                             (->> (str/join "/")))]
    (-> vs
        (assoc-in [:ftr :zen/package-name] zen-package-name)
        (assoc-in [:ftr :inferred-ftr-dir] inferred-ftr-dir))))


(defn build-ftr-index [ztx]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (map (partial zen.core/get-symbol ztx) syms)
        enriched-value-sets (map enrich-vs value-sets)
        ftr-cfgs-grouped-by-package-name
        (-> (group-by :ftr enriched-value-sets)
            keys
            (->>
              (filter identity)
              (group-by :zen/package-name))
            (update-vals (fn [cfgs]
                           (-> (group-by (juxt :tag :module :inferred-ftr-dir) cfgs)
                               keys
                               (->> (filter identity))))))
        tag-index-paths (filter (fn [{:keys [path]}]
                                  (.exists (io/file path)))
                                (mapcat (fn [[_package-name tag&module-pairs]]
                                          (map (fn [[tag module inferred-ftr-dir]]
                                                 {:tag tag
                                                  :module module
                                                  :ftr-dir inferred-ftr-dir
                                                  :path (format "%s/%s/tags/%s.ndjson.gz" inferred-ftr-dir module tag)})
                                               tag&module-pairs))
                                        ftr-cfgs-grouped-by-package-name))]

    (swap! ztx assoc :zen.fhir/ftr-index (index-by-tags tag-index-paths))))


(defn prefix? [a b]
  (= a (take (count a) b)))


(defn choose-validation-defer-to-use
  [defers]
  (reduce (fn [acc vs]
            (let [{:keys [result found]}
                  (some #(cond
                           (prefix? (:schemas %) (:schemas vs))
                           {:result :lose, :found %}

                           (prefix? (:schemas vs) (:schemas %))
                           {:result :win, :found %})
                        acc)]
              (case result
                :lose acc
                :win  (-> acc (conj vs) (disj found))
                (conj acc vs))))
          #{}
          defers))


(defn substack? [a b]
  (= a (take-last (count a) b)))


(defn filter-duplicate-value-sets [vs-group]
  (reduce (fn [group g-vs]
            (let [group-without-this (disj group g-vs)]
              (if (some #(substack? (:schemas g-vs) (:schemas %))
                        group-without-this)
                group-without-this
                group)))
          (set vs-group)
          vs-group))


(defn choose-bindings-to-use [bindings]
  (->> (group-by :value-set bindings )
       vals
       (mapcat filter-duplicate-value-sets)
       (choose-validation-defer-to-use)
       (group-by (juxt :path :value-set))
       vals
       (into #{}
             (map (fn [group]
                    (let [binding (select-keys (first group) #{:path :value-set :data :strength})
                          schemas (into #{} (map :schemas) group)]
                      (assoc binding :schemas schemas)))))) )


(defn normalize-value-set-data
  "Takes data which can be validated with the binding
  and transforms it into an array of Codings"
  [data]
  (cond
    (string? data) ;; FHIR code & string
    [{:code data}]

    (sequential? data) ;; FHIR CodeableConcept.coding
    (mapv #(select-keys % [:code :system])
          data)

    (and (map? data) (:coding data)) ;; FHIR CodeableConcept
    (mapv #(select-keys % [:code :system])
          (:coding data))

    (map? data) ;; FHIR Coding
    [(select-keys data [:code :system])]


    :else
    (assert false (str "Unknown data format: " (pr-str data)))
    #_data))


(defn validate-concept-via-ftr-index [ztx {{:as concept, :keys [code display]} :concept
                                           :keys [valueset valueset-ftr-tag]}]
  (let [ftr-index (get-in @ztx [:zen.fhir/ftr-index valueset-ftr-tag])
        codesystems-used-in-this-valueset (get-in ftr-index [:valuesets valueset])
        code-to-compare-with (->> codesystems-used-in-this-valueset
                                  (map (fn [cs] {:code (and (contains? (get-in ftr-index [:codesystems cs code :valueset]) valueset)
                                                            (get-in ftr-index [:codesystems cs code]))
                                                 :codesystem cs}))
                                  (filter :code)
                                  first)]
    (if display
      (= display (:display code-to-compare-with))
      code-to-compare-with)))


(defn validate-value-sets-via-ftr-index [ztx bindings]
  (let [concepts-to-validate (mapcat (fn [binding]
                                       (for [concept (:concepts binding)]
                                         {:binding_id (:id binding)
                                          :concept    concept
                                          :valueset (get-in binding [:value-set-sch :uri])
                                          :valueset-ftr-tag (get-in binding [:value-set-sch :ftr :tag])}))
                                     (vals bindings))]

    (reduce (fn [failed-concepts concept]
              (if (validate-concept-via-ftr-index ztx concept)
                failed-concepts
                (conj failed-concepts concept)))
            [] concepts-to-validate)))


(defn validate-value-sets [ztx zen-validation-acc]
  (when-let [bindings
             (some->> (get-in zen-validation-acc [:fx :zen.fhir/value-set :value-sets])
                      (group-by :path)
                      vals
                      (into {}
                            (comp (mapcat choose-bindings-to-use)
                                  (keep #(when-let [vs-sch (zen.core/get-symbol ztx (:value-set %))]
                                           (let [id (str (UUID/randomUUID))]
                                             [id (-> %
                                                     (assoc :value-set-sch vs-sch)
                                                     (assoc :concepts (normalize-value-set-data (:data %)))
                                                     (assoc :id id))])))))
                      not-empty)]
    (let [failed-checks (validate-value-sets-via-ftr-index ztx bindings)
          errors (for [[binding-id checks] (group-by :binding_id failed-checks)
                       :let [binding (get bindings binding-id)
                             all-checks-failed? (= (count (:concepts binding)) (count checks))]
                       :when all-checks-failed?
                       :let [code-systems-content-status (reduce (fn [acc {url :fhir/url, content :zen.fhir/content}]
                                                                   (assoc acc url content))
                                                                 {}
                                                                 (get-in binding [:value-set-sch :fhir/code-systems]))
                             all-systems-bundled?        (every? #(= :bundled %) (vals code-systems-content-status))
                             all-used-systems-bundled?   (every? (fn [{:keys [system]}]
                                                                   (or (and (some? system)
                                                                            (= :bundled (get code-systems-content-status system)))
                                                                       all-systems-bundled?))
                                                                 (:concepts binding))]
                       :when all-used-systems-bundled?]
                   {:type    ":zen.fhir/value-set"
                    :path    (:path binding)
                    :message (str "Expected '" (pr-str (:data binding))
                                  "' to be in the value set " (get-in binding [:value-set-sch :zen/name]))})]

      (-> zen-validation-acc
          (dissoc :value-sets)
          (update :errors (fnil into []) errors)) )))


(defn validate-effects [ztx result]
  (when ztx
    (validate-value-sets ztx result)))


(defn validate [ztx symbols data]
  (let [validation-result (zen.v2-validation/validate ztx symbols data)]
    (validate-effects ztx (dissoc validation-result :errors))))


(defn total-memory [obj]
  (let [baos (java.io.ByteArrayOutputStream.)]
    (with-open [oos (java.io.ObjectOutputStream. baos)]
      (.writeObject oos obj))
    (count (.toByteArray baos))))


(defn get-ftr-index-info
  ([args] (get-ftr-index-info (zen.cli/load-ztx args) args))
  ([ztx & _]
   (zen.cli/load-used-namespaces ztx #{})
   (doseq [[tag {:as ftr-index, :keys [valuesets codesystems]}] (get @ztx :zen.fhir/ftr-index)]
     (let [ftr-index-size-in-mbs                (int (/ (total-memory ftr-index) 1000000))
           amount-of-vs                     (count (keys valuesets))
           amount-of-cs                     (count (keys codesystems))
           cses                             (sort-by (comp count keys second) > codesystems)
           largest-cs                       (ffirst cses)
           amount-of-concepts-in-largest-cs (count (keys (second (first cses))))]
       (println (format "Tag: \033[0;1m%s\033[22m" tag))
       (println (format "    FTR index size: \033[0;1m%s MB\033[22m" ftr-index-size-in-mbs))
       (println (format "    ValueSets: \033[0;1m%s\033[22m" amount-of-vs))
       (println (format "    CodeSystems: \033[0;1m%s\033[22m" amount-of-cs))
       (println (format "    Largest CodeSystem: \033[0;1m%s\033[22m, \033[0;1m%s\033[22m codes" largest-cs amount-of-concepts-in-largest-cs))))))
