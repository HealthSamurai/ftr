(ns ftr.extraction.ig.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core]
            [edamame.core :as edamame]
            [com.rpl.specter :as sp]
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
       (mapcat (fn [dir] (when (dir? dir) (cons dir (.listFiles dir)))))
       (mapcat (fn [x] (if (and (dir? x) (str/starts-with? (.getName x) "@"))
                         (.listFiles x)
                         [x])))
       (filter dir?)
       distinct
       (filter (fn [f] (.exists (io/file (str (.getPath f) "/package.json")))))))


(defn init-ztx
  ([]
   (init-ztx (zen.core/new-context)))

  ([ztx]
   (zen.core/read-ns ztx 'zen.fhir)
   (swap! ztx assoc :zen.fhir/version (:zen.fhir/version (zen.core/get-symbol ztx 'zen.fhir/version)))
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
    :zen.fhir/package
    :zen.fhir/package-ns
    :zen.fhir/schema-ns
    :zen.fhir/file
    :zen.fhir/header
    :zen.fhir/version})


(defn blacklisted-package? [package]
  (contains? package-blacklist (:name package)))


(defmulti process-on-load
  (fn [res] (keyword (:resourceType res))))


(defmethod process-on-load :default
  [res])


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


(defn reduce-concept [acc id-fn sys parents c]
  (let [con (-> c
                (select-keys [:code :display :definition])
                (assoc :id (id-fn c)
                       :system sys
                       :_source "zen.fhir"
                       :resourceType "Concept")
                (cond-> (:designation c) (assoc :designation (build-designation (:designation c)))
                        (seq parents) (assoc :hierarchy parents)
                        (:property c) (assoc :property (build-property (:property c)))))
        acc (conj acc con)]
    (if-let [cs (:concept c)]
      (reduce (fn [acc c']
                (reduce-concept acc id-fn sys (conj parents (:code con)) c'))
              acc cs)
      acc)))


(defn extract-concepts [inter-part id-fn sys concept-parts]
  (->> concept-parts
       (reduce (fn [acc c] (reduce-concept acc id-fn sys [] c))
               [])
       (map (fn [concept]
              (-> concept
                  (merge inter-part)
                  (assoc :zen.fhir/resource concept))))))


(defmethod process-on-load :ValueSet
  [res]
  (merge
    res
    (when-let [package-ns (:zen.fhir/package-ns res)]
      {:zen.fhir/package-ns package-ns
       :zen.fhir/schema-ns (symbol (str (name package-ns) \. "value-set" \. (:id res)))
       :zen.fhir/resource (apply dissoc res loader-keys)
       :fhir/concepts (let [inter-part (select-keys res loader-keys)
                            concepts (->> (select-keys (:compose res) [:include :exclude])
                                          vals
                                          (apply concat)
                                          (filter :concept))
                            concepts (or (some->> [:expansion :contains]
                                                  (get-in res)
                                                  not-empty
                                                  (map #(assoc % :valueset [(:url res)]))
                                                  (group-by :system)
                                                  (reduce-kv (fn [acc system concepts]
                                                               (conj acc {:system system :concept concepts}))
                                                             [])
                                                  (into concepts))
                                         concepts)]
                        (->> concepts
                             (mapcat (fn [{:keys [system concept]}]
                                       (extract-concepts inter-part
                                                         (fn [{:keys [code]}] (str/replace (str system \/ code) \/ \-))
                                                         system
                                                         concept)))))})))


(defmethod process-on-load :CodeSystem
  [res]
  (merge
   (dissoc res :concept)
   {:fhir/concepts (extract-concepts (select-keys res loader-keys)
                                     (fn [{:keys [code]}] (str/replace (str (:url res) \/ code) \/ \-))
                                     (:url res)
                                     (:concept res))}
   {:zen.fhir/resource (apply dissoc res :concept loader-keys)}))


(defn load-definiton [ztx opts res]
  (let [rt (:resourceType res)
        url (or (:url res) (:url opts))]
    (if (and rt url)
      (when-let [processed-res (process-on-load res)]
        (swap! ztx update-in [:fhir/inter rt url]
               (fn [x]
                 (merge processed-res
                        {:_source "zen.fhir"
                         :zen.fhir/version (:zen.fhir/version @ztx)}
                        (select-keys res (conj loader-keys :_source))))))
      ())))


(defn do-load-file [ztx {:as opts :keys [whitelist blacklist params]} package f]
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


(defn preload-all [ztx & [{:keys [params node-modules-folder whitelist blacklist]
                           :or {node-modules-folder "node_modules"}}]]
  (init-ztx ztx)
  (doseq [pkg-dir  (find-packages node-modules-folder)]
    (let [package (read-json (str (.getPath pkg-dir) "/package.json"))
          package-params (get params (:name package))]
      (assert package (str "No package for " pkg-dir))
      (doseq [f (.listFiles pkg-dir)]
        (do-load-file ztx
                      {:params package-params
                       :whitelist whitelist
                       :blacklist blacklist}
                      package
                      f)))))


(defn collect-concepts [ztx]
  (let [code-systems (vals (get-in @ztx [:fhir/inter "CodeSystem"]))
        value-sets (vals (get-in @ztx [:fhir/inter "ValueSet"]))
        concepts (transduce (comp (mapcat :fhir/concepts)
                                  (map (fn [concept]
                                         {:path [(:system concept)
                                                 (:id concept)]
                                          :value concept})))
                            (completing
                              (fn [acc {:keys [path value]}]
                                (update-in acc path merge value)))
                            {}
                            (concat value-sets code-systems))]
    (swap! ztx assoc-in [:fhir/inter "Concept"] concepts)))


(defn process-concept [_ztx concept]
  (-> concept
      (assoc-in [:zen.fhir/resource :valueset]
                (vec (:valueset concept)))))


(defn process-concepts [ztx]
  (collect-concepts ztx)
  (ftr.extraction.ig.value-set-expand/denormalize-value-sets-into-concepts ztx)
  (swap! ztx update-in [:fhir/inter "Concept"]
         #(sp/transform [sp/MAP-VALS]
                        (partial process-concept ztx)
                        (reduce merge (vals %)))))


(defmethod u/*fn ::load-terminology [{:as _ctx, :keys [cfg ztx]}]
  (preload-all ztx cfg)
  (process-concepts ztx)
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
  (let [entire-cs-systems (->> index
                               keys
                               (filter (fn [k] (str/ends-with? k "-entire-code-system")))
                               (map (fn [v] (str/replace v "-entire-code-system" ""))))]
    (reduce
      (fn [acc sys]
        (let [vs-url (str sys "-entire-code-system")
              concepts (->> concepts
                            (filter (fn [[_ c]] (= sys (get-in c [:zen.fhir/resource :system]))))
                            (map (fn [[_ c]] (assoc-in c [:zen.fhir/resource :valueset] [vs-url]))))]
          (assoc-in acc [vs-url :concepts] concepts)))
      index entire-cs-systems)))


(defmethod u/*fn ::compose-tfs [{:as _ctx, :keys [ztx]}]
  (let [{value-sets "ValueSet"
         concepts "Concept"
         code-systems "CodeSystem"}
        (:fhir/inter @ztx)]
    {::result (re-check-entire-codesystem-valuesets
                (reduce-kv (fn [acc _concept-id concept]
                             (index-concepts-by-value-set acc concept value-sets code-systems))
                           {}
                           concepts)
                concepts)}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::load-terminology
                       ::compose-tfs]
                      {:ztx (zen-core/new-context {})
                       :cfg cfg})))
