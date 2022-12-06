(ns ftr.extraction.ig.value-set-expand
  (:require
   [zen.utils]
   [clojure.string :as str]))


(defn vs-compose-system-fn [ztx value-set system version]
  (when (some? system)
    (fn vs-compose-system [concept]
      (= system (:system concept)))))


(defn vs-compose-concept-fn [ztx value-set system version concepts]
  (when (seq concepts)
    (let [concept-codes (into #{} (map :code) concepts)]
      (fn vs-compose-concept [concept]
        (and (= system (:system concept))
             (contains? concept-codes (:code concept)))))))


(defmulti filter-op
  (fn [_ztx _value-set _system _version filter]
    (:op filter)))


(defmethod filter-op :default [_ztx _value-set _system _version _filter]
  (constantly false))


(defmethod filter-op "=" [_ztx _value-set _system _version filter]
  (fn eq-op [concept]
    (= (get (:property concept) (:property filter))
       (:value filter))))


(defmethod filter-op "in" [_ztx _value-set _system _version filter]
  (fn in-op [concept]
    (get (into #{} (map str/trim) (str/split (or (:value filter) "") #","))
         (get (:property concept) (:property filter)))))


(defmethod filter-op "not-in" [_ztx _value-set _system _version filter]
  (fn not-in-op [concept]
    (not (get (into #{} (map str/trim) (str/split (or (:value filter) "") #","))
              (get (:property concept) (:property filter))))))


(defmethod filter-op "exists" [_ztx _value-set _system _version filter]
  (if (= "false" (some-> (:value filter) str/lower-case str/trim))
    (fn not-exists-op [concept] (nil? (get (:property concept) (:property filter))))
    (fn exists-op [concept] (some? (get (:property concept) (:property filter))))))


(defmethod filter-op "is-a" [_ztx _value-set _system _version filter]
  (fn is-a-op [concept]
    (or (= (:code concept) (:value filter))
        (contains? (:zen.fhir/parents concept) (:value filter)))))


(defmethod filter-op "descendent-of" [_ztx _value-set _system _version filter]
  (fn descendatnd-of-op [concept]
    (contains? (set (:hierarchy concept)) (:value filter))))


(defmethod filter-op "is-not-a" [_ztx _value-set _system _version filter]
  (fn is-not-a-op [concept] ;; TODO: not sure this is correct impl by spec
    (and (not (contains? (set (:hierarchy concept)) (:value filter)))
         (not= (:code concept) (:value filter)))))


(defmethod filter-op "regex" [_ztx _value-set _system _version filter]
  (fn regex-op [concept]
    (when-let [prop (get (:property concept) (:property filter))]
      (re-matches (re-pattern (:value filter))
                  (str prop)))))


(defn vs-compose-filter-fn [ztx value-set system version filters]
  (when (seq filters)
    (let [filter-fn (->> filters
                         (map #(filter-op ztx value-set system version %))
                         (apply every-pred))]
      (fn compose-filter [concept]
        (and (= system (:system concept))
             (filter-fn concept))))))


(declare compose)


(defn vs-compose-value-set-fn [ztx value-set value-set-urls]
  (when-let [composes
             (some->> value-set-urls
                      (keep #(when-let [vs (get-in @ztx [:fhir/inter "ValueSet" %])]
                               (compose ztx vs)))
                      not-empty)]
    (when-let [check-fn
               (some->> composes
                        (keep :check-concept-fn)
                        not-empty
                        (apply every-pred))]
      {:systems (mapcat :systems composes)
       :check-fn check-fn})))


(defn check-if-concept-is-in-this-compose-el-fn [ztx value-set compose-el]
  (let [code-system-pred (or (vs-compose-concept-fn ztx value-set
                                                    (:system compose-el)
                                                    (:version compose-el)
                                                    (:concept compose-el))
                             (vs-compose-filter-fn ztx value-set
                                                   (:system compose-el)
                                                   (:version compose-el)
                                                   (:filter compose-el))
                             (vs-compose-system-fn ztx value-set
                                                   (:system compose-el)
                                                   (:version compose-el)))

        #_{value-set-systems :systems, value-set-pred :check-fn}
        #_(vs-compose-value-set-fn ztx value-set (:valueSet compose-el))

        check-fn code-system-pred
        #_(some->> [code-system-pred value-set-pred]
                   (remove nil?)
                   not-empty
                   (apply every-pred))]
    (if-let [vs-urls (:valueSet compose-el)]
      {:value-set-queue-entry {:vs-url (:url value-set)
                               :system (:system compose-el)
                               :check-fn (or check-fn (constantly true))
                               :depends-on vs-urls}}
      (when check-fn
        {:systems  [(:system compose-el)] #_(conj value-set-systems (:system compose-el))
         :check-fn check-fn}))))


(defn update-fhir-vs-expansion-index [ztx vs concept-identity-keys] #_"TODO: support recursive expansion :contains"
  (let [expansion-contains (get-in vs [:expansion :contains])
        full-expansion?    (and (= (count expansion-contains) (get-in vs [:expansion :total]))
                                (empty? (get-in vs [:expansion :parameter])))
        #_#_concepts (into #{} (map #(select-keys % concept-identity-keys)) expansion-contains)

        concepts-index (reduce (fn [acc concept]
                                 (assoc! acc (:system concept)
                                         (if-let [sys-idx (get acc (:system concept))]
                                           (conj! sys-idx (:code concept))
                                           (transient #{(:code concept)}))))
                               (transient {})
                               expansion-contains)]
    (swap! ztx assoc-in [:fhir/vs-expansion-index (:url vs)] {#_#_:concepts concepts
                                                              :concepts-transient-index concepts-index
                                                              :full?    full-expansion?})))


(defn compose [ztx vs]
  (let [concept-identity-keys [:code :system]
        _ (when (and (get-in vs [:expansion :contains])
                     (not (get-in @ztx [:fhir/vs-expansion-index (:url vs)])))
            (update-fhir-vs-expansion-index ztx vs concept-identity-keys))
        vs-url (:url vs)
        full-expansion? (get-in @ztx [:fhir/vs-expansion-index vs-url :full?])
        vs-concepts-index (-> @ztx
                              (get :fhir/vs-expansion-index)
                              (get vs-url)
                              (get :concepts-transient-index))
        expansion-fn (fn expansion-fn [{concept :zen.fhir/resource}]
                       (-> vs-concepts-index (get (:system concept)) (get (:code concept))))
        includes   (some->> (get-in vs [:compose :include])
                            (keep (partial check-if-concept-is-in-this-compose-el-fn ztx vs))
                            not-empty)
        include-depends (keep :value-set-queue-entry includes)
        include-fn (or (some->> includes
                                (keep :check-fn)
                                not-empty
                                (apply some-fn))
                       (constantly false))

        excludes   (some->> (get-in vs [:compose :exclude])
                            (keep (partial check-if-concept-is-in-this-compose-el-fn ztx vs))
                            not-empty)
        exclude-depends (keep :value-set-queue-entry excludes)
        exclude-fn (or (some->> excludes
                                (keep :check-fn)
                                not-empty
                                (apply some-fn)
                                complement)
                       (constantly true))

        include-and-exclude-fn (every-pred include-fn exclude-fn)

        check-concept-fn (if full-expansion?
                           expansion-fn
                           (some-fn expansion-fn include-and-exclude-fn))

        includes-and-excludes (concat includes excludes)

        systems (into #{}
                      (mapcat (comp not-empty :systems))
                      includes-and-excludes)]
    {:systems          (not-empty systems)
     :include-depends include-depends
     :exclude-depends exclude-depends
     :check-concept-fn check-concept-fn}))


(defn push-entries-to-vs-queue [queue entries entry-type]
  (reduce
    (fn [acc {:keys [vs-url depends-on system check-fn]}]
      (reduce #(update-in %1
                          [vs-url system %2]
                          conj
                          (case entry-type
                            :include check-fn
                            :exclude (complement check-fn)))
              acc
              depends-on))
    queue
    entries))


(defn dissoc-in&sanitize-maps [m path] #_"TODO: refactor"
  (assert (not= 0 (count path)) "Can't dissoc-in from empty path")

  (not-empty (zen.utils/dissoc-when
               empty?
               (reduce (fn [m-acc path]
                         (update-in m-acc (butlast path) #(zen.utils/dissoc-when empty? % (last path))))
                       (cond-> m
                         (< 1 (count path))
                         (update-in (butlast path) dissoc (last path)))
                       (take-while #(< 1 (count %)) (iterate butlast path)))
               (first path))))


(defn process-vs-nested-refs [acc vs-url]
  (reduce-kv
    (fn [acc dep-system-url depends-valuesets-idx]
      (reduce-kv
        (fn [acc depends-on-vs-url check-fns]
          (let [has-transitive-deps?
                (seq (get-in acc [:refs-queue depends-on-vs-url]))

                acc (if has-transitive-deps?
                      (process-vs-nested-refs acc depends-on-vs-url)
                      acc)

                acc (reduce
                      (fn [acc sys]
                        (let [vs-dep-sys-idx (get-in acc [:value-set-idx depends-on-vs-url sys])]
                          (reduce (fn [acc code]
                                    (let [concept (get-in acc [:concepts-map sys code])]
                                      (cond-> acc
                                        (every? #(% concept) check-fns)
                                        (update-in [:concepts-map sys code :valueset]
                                                   (fnil conj #{})
                                                   vs-url))))
                                  (update-in acc [:value-set-idx vs-url sys] #(into vs-dep-sys-idx %))
                                  vs-dep-sys-idx)))
                      (dissoc-in&sanitize-maps acc [:refs-queue vs-url dep-system-url depends-on-vs-url])
                      (if (some? dep-system-url)
                        [dep-system-url]
                        (keys (get-in acc [:value-set-idx depends-on-vs-url]))))]
            acc))
        acc
        depends-valuesets-idx))
    acc
    (get-in acc [:refs-queue vs-url])))


(defn process-nested-vss-refs [concepts-map value-set-idx nested-vs-refs-queue]
  (loop [acc {:refs-queue    nested-vs-refs-queue
              :concepts-map  concepts-map
              :value-set-idx value-set-idx}
         vs-url (ffirst nested-vs-refs-queue)]
    (let [{:as res-acc, :keys [refs-queue]} (process-vs-nested-refs acc vs-url)]
      (if (seq refs-queue)
        (recur res-acc (ffirst refs-queue))
        (:concepts-map res-acc)))))


(defn denormalize-into-concepts [ztx valuesets concepts-map]
  (let [{:keys [concepts-map
                value-set-idx
                nested-vs-refs-queue]}
        (reduce
          (fn reduce-valuesets [acc vs]
            (let [{systems        :systems
                   concept-in-vs? :check-concept-fn
                   :keys [include-depends exclude-depends]}
                  (compose ztx vs)]
              (reduce
                (fn reduce-codesystems [acc [system concepts]]
                  (reduce
                    (fn reduce-concepts [acc [concept-id concept]]
                      (if (concept-in-vs? concept)
                        (-> acc
                            (update-in [:concepts-map system concept-id :valueset]
                                       (fnil conj #{})
                                       (:url vs))
                            (update-in [:value-set-idx (:url vs) system]
                                       (fnil conj #{})
                                       (:code concept)))
                        acc))
                    acc
                    concepts))
                (-> acc
                    (update :nested-vs-refs-queue push-entries-to-vs-queue include-depends :exclude)
                    (update :nested-vs-refs-queue push-entries-to-vs-queue exclude-depends :include))
                (select-keys (:concepts-map acc) systems))))
          {:concepts-map         concepts-map
           :value-set-idx        {}
           :nested-vs-refs-queue {}}
          valuesets)]
    (process-nested-vss-refs concepts-map value-set-idx nested-vs-refs-queue)))


(defn denormalize-value-sets-into-concepts [ztx]
  (swap! ztx update-in [:fhir/inter "Concept"]
         (partial denormalize-into-concepts
                  ztx (vals (get-in @ztx [:fhir/inter "ValueSet"])))))
