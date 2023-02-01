(ns ftr.extraction.ig.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core]
            [edamame.core :as edamame]
            [zen.core :as zen-core]
            [ftr.utils.unifn.core :as u]
            [ftr.utils.core]
            [ftr.extraction.ig.value-set-expand]))



(defn dir? [^java.io.File file]
  (and (.isDirectory file)
       (not (str/starts-with? (.getName file) "."))))


(defn find-packages [project-root]
  (->> [(io/file project-root)
        (io/file (str project-root "/node_modules"))]
       (mapcat (fn [^java.io.File dir] (when (dir? dir) (cons dir (.listFiles dir)))))
       (mapcat (fn [^java.io.File x] (if (and (dir? x) (str/starts-with? (.getName x) "@"))
                                       (.listFiles x)
                                       [x])))
       (filter dir?)
       distinct
       (filter (fn [^java.io.File f] (.exists (io/file (str (.getPath f) "/package.json")))))))


(defn init-ztx
  ([]
   (init-ztx (zen.core/new-context)))

  ([ztx]
   (swap! ztx assoc :zen.fhir/version
          (if-let [res (some-> (io/resource "zen-fhir-version")
                               slurp)]
            res
            (do
              (zen.core/read-ns ztx 'zen.fhir)
              (:zen.fhir/version (zen.core/get-symbol ztx 'zen.fhir/version)))))
   ztx))


(defn read-json [f] (cheshire.core/parse-string (slurp f) keyword))


(def package-blacklist
  #{"hl7.fhir.r2.examples"
    "hl7.fhir.r2b.examples"
    "hl7.fhir.r3.examples"
    "hl7.fhir.r4.examples"})


(def loader-keys
  #{:zen/loader
    :zen.fhir/loader
    :zen.fhir/parents
    :zen.fhir/code-kw
    :zen.fhir/system-kw
    :zen.fhir/package
    :zen.fhir/package-ns
    :zen.fhir/packages
    :zen.fhir/schema-ns
    :zen.fhir/file
    :zen.fhir/header
    :zen.fhir/version})


(defn blacklisted-package? [package]
  (contains? package-blacklist (:name package)))


(defmulti process-on-load
  (fn [res] (keyword (:resourceType res))))


(defmethod process-on-load :default
  [_res]
  #_(println :WARN :no-process-on-load :for (:resourceType res)))


(defn get-value [m]
  (let [k (->> (keys m)
               (filter #(str/starts-with? (name %) "value"))
               (first))]
    (get m k)))


(defn build-property [ps]
  (reduce (fn [acc p]
            (assoc acc (:code p) (get-value p)))
          {} ps))


(defn build-designation [ds]
  (reduce (fn [acc d]
            (assoc-in acc [(or (get-in d [:use :code]) "display")
                           (or (:language d) "en")]
                      (:value d)))
          {} ds))


(defn reduce-concept [acc id-fn sys vs hierarchy parents c]
  (let [con (-> c
                (select-keys [:code :display :definition])
                (assoc :id (id-fn c)
                       :system sys
                       :_source "zen.fhir"
                       :resourceType "Concept"
                       :zen.fhir/code-kw (keyword (:code c))
                       :zen.fhir/system-kw (keyword sys))
                (cond-> (:designation c) (assoc :designation (build-designation (:designation c)))
                        vs               (assoc :valueset #{vs})
                        (seq hierarchy) (assoc :hierarchy hierarchy)
                        (seq parents) (assoc :zen.fhir/parents parents)
                        (:property c) (assoc :property (build-property (:property c)))))
        acc (conj acc con)]
    (if-let [cs (:concept c)]
      (reduce (fn [acc c']
                (reduce-concept acc id-fn sys
                                vs
                                (conj hierarchy (:code con))
                                (conj parents (keyword (:code con)))
                                c'))
              acc cs)
      acc)))


(defn extract-concepts [inter-part id-fn sys concept-parts & [vs]]
  (->> concept-parts
       (reduce (fn [acc c] (reduce-concept acc id-fn sys vs [] #{} c))
               [])
       (map (fn [concept]
              (-> concept
                  (merge inter-part)
                  (assoc :zen.fhir/resource concept))))))


(defmethod process-on-load :ValueSet
  [res]
  (merge
    res
    {:zen.fhir/resource (apply dissoc res loader-keys)
     :fhir/concepts (let [inter-part (select-keys res loader-keys)
                          incl-concepts (->> (get-in res [:compose :include])
                                             (filter :concept)
                                             (map #(assoc % :vs (:url res))))
                          excl-concepts (->> (get-in res [:compose :exclude])
                                             (filter :concept))
                          exp-concepts (some->> [:expansion :contains]
                                                (get-in res)
                                                not-empty
                                                (group-by :system)
                                                (reduce-kv (fn [acc system concepts]
                                                             (conj acc {:system system
                                                                        :vs (:url res)
                                                                        :concept concepts}))
                                                           []))]
                      (->> (concat incl-concepts excl-concepts exp-concepts)
                           (mapcat (fn [{:keys [vs system concept]}]
                                     (extract-concepts inter-part
                                                       (fn [{:keys [code]}] (str/replace (str system \/ code) \/ \-))
                                                       system
                                                       concept
                                                       vs)))
                           not-empty))}
    (when-let [package-ns (:zen.fhir/package-ns res)]
      {:zen.fhir/package-ns package-ns
       :zen.fhir/schema-ns (symbol (str (name package-ns) \. "value-set" \. (:id res)))})))


(defmethod process-on-load :CodeSystem
  [res]
  (merge
   (dissoc res :concept)
   {:fhir/concepts (extract-concepts (select-keys res loader-keys)
                                     (fn [{:keys [code]}] (str/replace (str (:url res) \/ code) \/ \-))
                                     (:url res)
                                     (:concept res))}
   {:zen.fhir/resource (apply dissoc res :concept loader-keys)}))


#_"NOTE: We know that hl7.fhir.r4.core ValueSet resources are clashing by url with hl7.terminology.r4 ValueSets resources.
   Currently we always choose hl7.terminology.r4 ValueSets over those from hl7.fhir.r4.core.
   We assume that there are no other ValueSets clashing.
   If we find such a clash, we throw an exception to make it noticeable."
(def package-priority
  {:hl7.terminology.r4 #{:hl7.fhir.r4.core :hl7.fhir.us.carin-bb :hl7.fhir.us.davinci-pdex}})


(defn clash-ex-data [code old new]
  {:code code
   :old  {:rt      (:resourceType old)
          :url     (:url old)
          :package (get-in old [:zen/loader :package :name])
          :file    (get-in old [:zen/loader :file])}
   :new  {:rt      (:resourceType new)
          :url     (:url new)
          :package (get-in new [:zen/loader :package :name])
          :file    (get-in new [:zen/loader :file])}})


(defn resolve-clash-dispatch [rt _old _new]
  (keyword rt))


(defmulti resolve-clash #'resolve-clash-dispatch)


(defmethod resolve-clash :default [_rt old new]
  (throw (Exception. (str (clash-ex-data ::no-resolve-clash-rules-defined old new)))))


(defn get-package-priority [_rt priority-map old new]
  (let [old-package  (keyword (get-in old [:zen/loader :package :name]))
        new-package  (keyword (get-in new [:zen/loader :package :name]))]
    {:old (get-in priority-map [old-package new-package])
     :new (get-in priority-map [new-package old-package])}))


(defn decide-on-clash [compare-key old new]
  (let [compare-res (compare (compare-key old)
                             (compare-key new))]
    (cond
      (neg? compare-res)  ::override-with-higher-priority
      (zero? compare-res) ::unresolved-clash
      (pos? compare-res)  ::skip-lower-priority)))


(defmethod resolve-clash :CodeSystem [rt old new]
  (let [status-weight  {:active  0
                        :draft   -10
                        :unknown -20
                        :retired -30}
        content-weight {:complete    0
                        :fragment    -10
                        :supplement  -20
                        :example     -30
                        :not-present -40}

        {old-priority :old, new-priority :new}
        (get-package-priority rt package-priority old new)

        result (decide-on-clash (juxt #(get status-weight (keyword (:status %)))
                                      #(get content-weight (keyword (:content %)))
                                      :priority)
                                (assoc old :priority old-priority)
                                (assoc new :priority new-priority))]
    (if (and (= ::unresolved-clash result)
             (= "not-present" (:content new) (:content old)))
      (decide-on-clash (juxt :date :version #_#(get-in % [:zen/loader :package :name]))
                       old
                       new)
      result)))


(defmethod resolve-clash :ValueSet [rt old new]
  (let [{old-priority :old, new-priority :new} (get-package-priority rt package-priority old new)]
    (decide-on-clash (juxt :priority)
                     (assoc old :priority old-priority)
                     (assoc new :priority new-priority))))


(defn check-priority [inter-old inter-new]
  (let [rt-kw (keyword (:resourceType inter-old))]
    (if (nil? inter-old)
      ::no-clash
      (resolve-clash rt-kw inter-old inter-new))))


(defn ensure-no-clash [old new]
  (case (check-priority old new)
    (::no-clash ::override-with-higher-priority)
    new

    ::skip-lower-priority
    old

    ::unresolved-clash
    (throw (Exception. (str (clash-ex-data ::unresolved-clash old new))))))


(defn load-definition [ztx {:as opts, :keys [skip-concept-processing]} res]
  (let [rt  (:resourceType res)
        url (or (:url res) (:url opts))]
    (if (or (nil? url) (nil? rt))
      (println :skip-resource "no url or rt" (get-in res [:zen/loader :file]))
      (when-let [processed-res (process-on-load res)]
        (let [processed-res (cond-> processed-res
                              (and (comp #{"CodeSystem" "ValueSet"} :resourceType)
                                   skip-concept-processing)
                              (assoc :fhir/concepts '()))
              processed-res (merge processed-res
                                   {:_source          "zen.fhir"
                                    :zen.fhir/version (:zen.fhir/version @ztx)}
                                   (select-keys res (conj loader-keys :_source)))]
          (swap! ztx update-in [:fhir/inter rt url] ensure-no-clash processed-res))))))


(def load-definiton load-definition)


(defn do-load-file [ztx {:as opts :keys [whitelist blacklist params]} package ^java.io.File f]
  (let [file-name (.getName f)
        content (cond
                  (str/ends-with? file-name ".json")
                  (try (cheshire.core/parse-string (str/replace (slurp f) \ufeff \space) keyword)
                       (catch Exception e
                         (println :WARN :invalid-json (.getName f) (.getMessage e))))

                  (str/ends-with? file-name ".edn")
                  (edamame/parse-string (slurp f)))
        rt-whitelist (get whitelist (:resourceType content))
        rt-blacklist (get blacklist (:resourceType content))]
    (when (and (not (blacklisted-package? package))
               content
               (or (nil? rt-blacklist)
                   (not (contains? rt-blacklist (:url content))))
               (or (nil? rt-whitelist)
                   (contains? rt-whitelist (:url content))))
      (load-definiton ztx opts (assoc content
                                      :_source "zen.fhir"
                                      :zen.fhir/version (:zen.fhir/version @ztx)
                                      :zen/loader {:package package :file (.getPath f)}
                                      :zen.fhir/package package
                                      :zen.fhir/file (.getPath f)
                                      :zen.fhir/package-ns (or (:zen.fhir/package-ns params)
                                                               (some-> package :name (str/replace #"\." "-") symbol)))))))


(defn preload-all [ztx & [{:keys [params node-modules-folder whitelist blacklist skip-concept-processing]
                           :or {node-modules-folder "node_modules"}}]]
  (init-ztx ztx)
  (doseq [^java.io.File pkg-dir  (find-packages node-modules-folder)]
    (let [package (read-json (str (.getPath pkg-dir) "/package.json"))
          package-params (get params (:name package))]
      (assert package (str "No package for " pkg-dir))
      (doseq [f (.listFiles pkg-dir)]
        (do-load-file ztx
                      {:params package-params
                       :skip-concept-processing skip-concept-processing
                       :whitelist whitelist
                       :blacklist (merge-with merge
                                              {"StructureDefinition" #{"http://hl7.org/fhir/StructureDefinition/familymemberhistory-genetic"
                                                                       "http://hl7.org/fhir/uv/sdc/StructureDefinition/parameters-questionnaireresponse-extract-in"}
                                               "SearchParameter" #{"http://hl7.org/fhir/SearchParameter/example"}
                                               "ValueSet" #{"http://hl7.org/fhir/ValueSet/example-expansion"
                                                            "http://hl7.org/fhir/ValueSet/example-hierarchical"}}
                                              blacklist)}
                      package
                      f)))))


(defn collect-concepts [ztx]
  (let [code-systems (vals (get-in @ztx [:fhir/inter "CodeSystem"]))
        value-sets (vals (get-in @ztx [:fhir/inter "ValueSet"]))
        concepts (transduce (comp (mapcat :fhir/concepts)
                                  (map (fn [concept]
                                         {:path [(:system concept)
                                                 (:code concept)]
                                          :value concept})))
                            (completing
                              (fn [acc {:keys [path value]}]
                                (update-in acc path
                                           (fn [prev-val]
                                             (-> prev-val
                                                 (merge value)
                                                 (update :zen.fhir/packages
                                                         (fnil conj #{})
                                                         (:zen.fhir/package-ns value)))))))
                            {}
                            (concat value-sets code-systems))]
    (swap! ztx assoc-in [:fhir/inter "Concept"] concepts)))


(defn process-concept [_ztx concept]
  (-> concept
      (assoc-in [:zen.fhir/resource :valueset]
                (vec (:valueset concept)))
      (update-in [:zen.fhir/resource] #(apply dissoc % loader-keys))))


(defn flatten-nested-map [m levels key-concat-fn & [ks]]
  (if (< 1 levels)
    (into {}
          (map (fn [[k v]] (flatten-nested-map v (dec levels) key-concat-fn (conj (or ks []) k))))
          m)
    (into {}
          (map (fn [[k v]] [(key-concat-fn (conj ks k))
                            v]))
          m)))


(defn process-concepts [ztx]
  (collect-concepts ztx)
  (ftr.extraction.ig.value-set-expand/denormalize-value-sets-into-concepts ztx)
  (swap! ztx update-in [:fhir/inter "Concept"]
         (fn [codesystems-concepts]
           (update-vals (flatten-nested-map
                          codesystems-concepts
                          2
                          (fn [[system code]] (str system \- code)))
                        #(process-concept ztx %)))))


(defn process-resource-type-dispatch [rt _ztx & [_params]] rt)


(defmulti process-resource-type #'process-resource-type-dispatch)


(defmethod process-resource-type :Concept [_ ztx & [{:as _params, :keys [skip-concept-processing]}]]
  (when-not skip-concept-processing
    (process-concepts ztx)))


(defn process-resources [ztx & [{:as params, :keys [types]}]]
  (doseq [[rt method] (cond-> (methods process-resource-type)
                        (seq types) (select-keys types))]
    (method rt ztx params)))


(defn load-all [ztx _ & [params]]
  (preload-all ztx params)
  (process-resources ztx params)
  :done)


(defmethod u/*fn ::load-terminology [{:as _ctx, :keys [cfg ztx]}]
  (load-all ztx nil (merge cfg {:types #{:Concept}}))
  {})


(defn get-or-create-codesystem [code-systems system]
  (if-let [cs (get-in code-systems [system :zen.fhir/resource])]
    cs
    {:url system
     :name (ftr.utils.core/escape-url system)
     :content "not-present"
     :status "unknown"
     :resourceType "CodeSystem"}))


(defn create-vs-for-entire-cs [code-system]
  (ftr.utils.core/strip-nils
    {:url    (some-> (:url code-system) (str "-entire-code-system"))
     :name   (some-> (:name code-system) (str "-entire-code-system"))
     :status "unknown"
     :resourceType "ValueSet"}))


(defn preprocess-concept [concept vs-url]
  (-> concept
      (assoc :valueset [vs-url]) #_"Trim backrefs to other valuesets"
      (assoc :id (str (ftr.utils.core/escape-url vs-url)
                      "-"
                      (:code concept)))))


(defn index-concepts-by-value-set-backref [vs-idx-acc vs-url concept system value-sets code-systems]
  (let [code-system (get-or-create-codesystem code-systems system)
        _ (assert (not (nil? (:url code-system))))
        concept (preprocess-concept
                  (get concept :zen.fhir/resource)
                  vs-url)]
    (if (contains? vs-idx-acc vs-url)
      (-> vs-idx-acc
          (update-in [vs-url :code-system] conj code-system)
          (update-in [vs-url :concepts] conj concept))
      (let [value-set   (get-in value-sets [vs-url :zen.fhir/resource])]
        (assoc vs-idx-acc vs-url {:concepts [concept]
                                  :code-system #{code-system}
                                  :value-set value-set})))))


(defn index-concepts-by-vs-for-entire-cs [vs-idx-acc concept system code-systems]
  (let [code-system                  (get-or-create-codesystem code-systems system)
        {:as value-set, vs-url :url} (create-vs-for-entire-cs code-system)
        _ (assert (not (nil? (:url code-system))))
        concept (preprocess-concept
                  (get concept :zen.fhir/resource)
                  vs-url)]
    (if (contains? vs-idx-acc vs-url)
      (-> vs-idx-acc
          (update-in [vs-url :code-system] conj code-system)
          (update-in [vs-url :concepts] conj concept))
      (assoc vs-idx-acc vs-url {:concepts [concept]
                                :code-system #{code-system}
                                :value-set value-set}))))


(defn index-concepts-by-value-set [vs-idx-acc {:as concept, :keys [valueset system]} value-sets code-systems]
  (if (seq valueset)
    (reduce (fn [vs-idx-acc' vs-url]
              (index-concepts-by-value-set-backref vs-idx-acc' vs-url concept system value-sets code-systems))
            vs-idx-acc
            valueset)
    (index-concepts-by-vs-for-entire-cs vs-idx-acc concept system code-systems)))


(defn re-check-entire-codesystem-valuesets [index concepts]
  (let [entire-cs-systems (->> (dissoc index nil)
                               keys
                               (filter (fn [k] (str/ends-with? k "-entire-code-system")))
                               (map (fn [v] (str/replace v "-entire-code-system" ""))))]
    (reduce
      (fn [acc sys]
        (let [vs-url (str sys "-entire-code-system")
              concepts (->> concepts
                            (filter (fn [[_ c]] (= sys (get-in c [:zen.fhir/resource :system]))))
                            (map (fn [[_ c]] (:zen.fhir/resource (assoc-in c [:zen.fhir/resource :valueset] [vs-url])))))]
          (assoc-in acc [vs-url :concepts] concepts)))
      index entire-cs-systems)))


(defmethod u/*fn ::compose-tfs [{:as _ctx, :keys [ztx]}]
  (let [{value-sets "ValueSet"
         concepts "Concept"
         code-systems "CodeSystem"}
        (:fhir/inter @ztx)]
    {::result (dissoc (re-check-entire-codesystem-valuesets
                        (reduce-kv (fn [acc _concept-id concept]
                                     (index-concepts-by-value-set acc concept value-sets code-systems))
                                   {}
                                   concepts)
                        concepts) nil)}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::load-terminology
                       ::compose-tfs]
                      {:ztx (zen-core/new-context {})
                       :cfg cfg})))



