(ns ftr.zen-package
  (:require [zen.core]
            [cheshire.core]
            [ftr.core]
            [ftr.utils.core]
            [clojure.string :as str]
            [zen.v2-validation]
            [clojure.java.io :as io]
            [zen.cli])
  (:import (java.util UUID)))


(defn build-ftr [ztx & [{:as opts, :keys [enable-logging?]}]]
  (let [syms (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets (->> syms
                        (map (partial zen.core/get-symbol ztx))
                        (remove (fn [{:as _vs-definition,
                                      :zen/keys [file]}]
                                  (str/includes? file "/zen-packages/"))))
        ftr-configs (->> value-sets
                         (group-by :ftr)
                         keys
                         (filter identity))]
    (doseq [ftr ftr-configs]
      (ftr.core/apply-cfg (cond-> {:cfg ftr}
                            enable-logging?
                            (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))))))


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


(defn read-ndjson-line! [^java.io.BufferedReader buffered-reader]
  (cheshire.core/parse-string (.readLine buffered-reader) keyword))


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
                  (ftr.utils.core/open-gz-reader tf-path)

                  {codesystems "CodeSystem"
                   [{vs-url :url}] "ValueSet"}
                  (loop [line (read-ndjson-line! new-tf-reader)
                         css&vs []]
                    (if (= (:resourceType line) "ValueSet")
                      (group-by :resourceType (conj css&vs line))
                      (recur (read-ndjson-line! new-tf-reader)
                             (conj css&vs line))))

                  codesystems-urls
                  (map :url codesystems)

                  ftr-index-by-tag-with-updated-vss
                  (if (contains? valuesets vs-url)
                    (update-in ftr-index-by-tag [:valuesets vs-url] into codesystems-urls)
                    (assoc-in ftr-index-by-tag [:valuesets vs-url] (set codesystems-urls)))]
              (loop [{:as concept, :keys [code system display]}
                     (read-ndjson-line! new-tf-reader)

                     {:as ftr-index-by-tag, :keys [codesystems]}
                     ftr-index-by-tag-with-updated-vss]

                (if-not (nil? concept)
                  (recur
                    (read-ndjson-line! new-tf-reader)
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
  (if (contains? vs :ftr)
    (let [{:as _ftr-manifest,
           :keys [ftr-path source-type source-url]}
          (get vs :ftr)
          zen-file         (:zen/file vs)
          path-to-package  (->> (str/split zen-file #"/")
                                (take-while (complement #{"zrc"})))
          zen-package-name (last path-to-package)
          inferred-ftr-dir (if (= source-type :cloud-storage) ;;TODO Re-design manifests, harmonize source-types on ftr design/runtime phase
                             (str source-url \/ ftr-path) ;;That's uncorrect, cause source-url intended to store path to raw-terminology source,
                                                          ;;same thing with the source-type.
                             (-> path-to-package
                                 vec
                                 (conj "ftr")
                                 (->> (str/join "/"))))]
      (-> vs
          (assoc-in [:ftr :zen/package-name] zen-package-name)
          (assoc-in [:ftr :inferred-ftr-dir] inferred-ftr-dir)))
    vs))


(defn url? [path]
  (let [url (try (java.net.URL. path)
                 (catch Exception _ false))]
    (instance? java.net.URL url)))


(defn build-complete-ftr-index [ztx]
  (let [syms                (zen.core/get-tag ztx 'zen.fhir/value-set)
        value-sets          (map #(zen.core/get-symbol ztx %) syms)
        enriched-value-sets (map enrich-vs value-sets)

        ftr-cfgs-grouped-by-package-name
        (->> (keep :ftr enriched-value-sets)
             distinct
             (group-by :zen/package-name))

        tag-index-paths
        (for [[_package-name tag&module-pairs]      ftr-cfgs-grouped-by-package-name
              {:keys [module tag inferred-ftr-dir]} (->> tag&module-pairs
                                                         (keep #(not-empty (select-keys % [:tag :module :inferred-ftr-dir])))
                                                         distinct)

              :let  [path (format "%s/%s/tags/%s.ndjson.gz" inferred-ftr-dir module tag)]
              :when (or (.exists (io/file path))
                        (url? path))]

          {:tag     tag
           :module  module
           :ftr-dir inferred-ftr-dir
           :path    path})]

    (swap! ztx assoc :zen.fhir/ftr-index {:result    (index-by-tags tag-index-paths)
                                          :complete? true})))


(defn- enrich-ftr-index-with-tf [ftr-index ftr-tag tf-path]
  "Enrich FTR index with terminology file"
  (let [tf-reader
        (ftr.utils.core/open-ndjson-gz-reader tf-path)

        {codesystems "CodeSystem"
         [{vs-url :url}] "ValueSet"}
        (loop [line (.readLine tf-reader)
               css&vs []]
          (if (= (:resourceType line) "ValueSet")
            (group-by :resourceType (conj css&vs line))
            (recur (.readLine tf-reader) (conj css&vs line))))

        codesystems-urls
        (map :url codesystems)

        ftr-index-with-updated-vss
        (update-in ftr-index
                   [ftr-tag :valuesets vs-url]
                   (fnil into #{})
                   codesystems-urls)]

    (loop [{:as concept, :keys [code system display]}
           (.readLine tf-reader)

           ftr-index ftr-index-with-updated-vss]
      (if-not (nil? concept)
        (recur
         (.readLine tf-reader)
         (update-in ftr-index
                    [ftr-tag :codesystems system code]
                    (fn [code-idx]
                      (if (some? code-idx)
                        (update code-idx :valueset conj vs-url)
                        {:display  display
                         :valueset #{vs-url}}))))
        ftr-index))))


(defn- find-hash-for-name-in-tag-index [tag-index name]
  (->> tag-index
       (filter #(= (:name %) name))
       first
       :hash))


(defn enrich-ftr-index-with-vs [ztx vs-sym]
  (let [vs-sch
        (zen.core/get-symbol ztx vs-sym)

        {:as enriched-vs-sch,
         vs-uri :uri
         {ftr-tag    :tag
          ftr-module :module
          ftr-path   :inferred-ftr-dir} :ftr}
        (enrich-vs vs-sch)

        tag-index
        (when (every? some? [ftr-path ftr-module ftr-tag])
          (-> (format "%s/%s/tags/%s.ndjson.gz" ftr-path ftr-module ftr-tag)
              ftr.utils.core/parse-ndjson-gz))]
    (when tag-index
      (let [ftr-vs-name
            (ftr.utils.core/escape-url vs-uri)

            ftr-vs-hash
            (find-hash-for-name-in-tag-index tag-index (format "%s.%s" ftr-module ftr-vs-name))

            tf-path
            (format "%s/%s/vs/%s/tf.%s.ndjson.gz"
                    ftr-path
                    ftr-module
                    ftr-vs-name
                    ftr-vs-hash)]
        (swap! ztx update-in [:zen.fhir/ftr-index :result] enrich-ftr-index-with-tf ftr-tag tf-path)))))


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
  (let [ftr-index (get-in @ztx [:zen.fhir/ftr-index :result valueset-ftr-tag])
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
  ([opts] (get-ftr-index-info (zen.cli/load-ztx opts) opts))
  ([ztx & _]
   (zen.cli/load-used-namespaces ztx #{})
   {:result
    (update-vals (get-in @ztx [:zen.fhir/ftr-index :result])
                 (fn [{:as ftr-index, :keys [valuesets codesystems]}]
                   (let [ftr-index-size-in-mbs            (int (/ (total-memory ftr-index) 1000000))
                         amount-of-vs                     (count valuesets)
                         amount-of-cs                     (count codesystems)
                         largest-cs                       (apply max-key (comp count val) codesystems)
                         amount-of-concepts               (transduce (map count) + 0 (vals codesystems))
                         amount-of-concepts-in-largest-cs (count (keys (val largest-cs)))]
                     {:ftr-index           {:size ftr-index-size-in-mbs, :unit :MB}
                      :value-sets          amount-of-vs
                      :code-systems        amount-of-cs
                      :concepts            amount-of-concepts
                      :largest-code-system {:code-system (key largest-cs)
                                            :concepts    amount-of-concepts-in-largest-cs}})))}))


(comment
  (def a
    (time (ftr.utils.core/parse-ndjson-gz "/Users/ghrp/ftr/snomed/vs/http:--snomed.info-sct/tf.1ccf5ffb3e489e1b9aefd51160158d87f962a4b681ea77353077ff0edde64d27.ndjson.gz")))

  (count a)

  (int (/ (total-memory a) 1000000))

  )
