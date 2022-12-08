(ns ftr.extraction.ig.value-set-expand
  (:require
   [zen.utils]
   [clojure.set :as set]
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
      {:value-set-queue-entry {:vs-url     (:url value-set)
                               :system     (:system compose-el)
                               :check-fn   check-fn
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
      (update-in acc
                 [vs-url (or system ::any-system) entry-type depends-on]
                 conj
                 (or check-fn ::any-concept)))
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


(defn pop-entry-from-vs-queue [acc vs-url sys-url]
  (dissoc-in&sanitize-maps acc [:refs-queue vs-url sys-url]))


(defn push-concept-into-vs-idx [vs-idx vs-url concept]
  (update-in vs-idx
             [vs-url (:system concept)]
             (fnil conj #{})
             (:code concept)))


(defn get-acc-key [any-system? mode]
  (case [any-system? mode]
    [true :include]  :any-sys-include-acc
    [false :include] :include-acc
    [true :exclude]  :any-sys-exclude-acc
    [false :exclude] :exclude-acc))


(defn vs-selected-system-intersection->vs-idx [acc vs-url sys vs-urls checks vs-idx concepts-map & {:keys [any-system? mode]}]
  (if-let [codes (->> vs-urls
                      (map (fn [dep-vs-url]
                             (set/union (get-in acc [:vs-idx-acc dep-vs-url sys])
                                        (get-in vs-idx [dep-vs-url sys]))))
                      (apply set/intersection)
                      not-empty)]
    (let [acc-key (get-acc-key any-system? mode)]
      (if-let [check-fns (not-empty (remove #(= ::any-concept %) checks))]
        (update acc acc-key
                (fn [vs-idx-acc]
                  (transduce (comp (map (fn [code] (get-in concepts-map [sys code])))
                                   (filter (fn [concept] (every? #(% concept) check-fns))))
                             (completing (fn [acc concept] (push-concept-into-vs-idx acc vs-url concept)))
                             vs-idx-acc
                             codes)))
        (update-in acc [acc-key vs-url sys] (fnil into #{}) codes)))
    acc))


(defn select-all-dep-systems [vs-idx-acc vs-idx deps-vs-urls]
  (mapcat #(concat (keys (get vs-idx-acc %))
                   (keys (get vs-idx %)))
          deps-vs-urls))


(defn vs-selected-systems->mode-acc [acc vs-url dep-system-url vs-urls check-fns vs-idx concepts-map mode]
  (let [any-system? (= ::any-system dep-system-url)
        selected-systems (if any-system?
                           (select-all-dep-systems (:vs-idx-acc acc) vs-idx vs-urls)
                           [dep-system-url])]
    (reduce (fn [acc sys]
              (vs-selected-system-intersection->vs-idx acc vs-url sys vs-urls check-fns vs-idx concepts-map
                                                       {:any-system? any-system?
                                                        :mode mode}))
            acc
            selected-systems)))


(declare refs-in-vs->vs-idx)


(defn ensure-deps-processed [acc vs-urls vs-idx concepts-map]
  (transduce (filter #(seq (get-in acc [:refs-queue %])))
             (completing #(refs-in-vs->vs-idx %1 %2 vs-idx concepts-map))
             acc
             vs-urls))


(defn collect-mode-acc [acc vs-url system include concepts-map vs-idx mode]
  (reduce-kv (fn [acc vs-urls check-fns]
               (vs-selected-systems->mode-acc acc vs-url system vs-urls check-fns vs-idx concepts-map mode))
             acc
             include))


(defn push-include-exclude->vs-idx [acc vs-url dep-system-url concepts-map vs-idx]
  (let [any-system?    (= ::any-system dep-system-url)
        incl-acc-key   (get-acc-key any-system? :include)
        excl-acc-key   (get-acc-key any-system? :exclude)
        vs-sys-idx-acc (get-in acc [:vs-idx-acc vs-url])
        exclude-idx    (get-in acc [excl-acc-key vs-url])
        exclude-filter (when (seq exclude-idx)
                         (fn [system code]
                           (not (get-in exclude-idx [system code]))))

        new-vs-sys-idx
        (if any-system?
          (if exclude-filter
            (reduce-kv (fn [acc sys concepts]
                         (update acc sys (fn [idx-concepts]
                                           (into (or idx-concepts #{})
                                                 (filter #(exclude-filter sys %))
                                                 concepts))))
                       vs-sys-idx-acc
                       (get-in acc [incl-acc-key vs-url]))
            (merge-with #(if (some? %1)
                           (into %1 %2)
                           %2)
                        vs-sys-idx-acc
                        (get-in acc [incl-acc-key vs-url])))
          (update vs-sys-idx-acc
                  dep-system-url
                  (if exclude-filter
                    (fn [idx-concepts new-concepts]
                      (into (or idx-concepts #{})
                            (filter #(exclude-filter dep-system-url %))
                            new-concepts))
                    #(if (some? %1)
                       (into %1 %2)
                       %2))
                  (get-in acc [incl-acc-key vs-url dep-system-url])))]
    (assoc-in acc [:vs-idx-acc vs-url] new-vs-sys-idx)))


(defn refs-in-vs->vs-idx [acc vs-url vs-idx concepts-map]
  (reduce-kv (fn [acc dep-system-url {:keys [include exclude]}]
               (let [deps (distinct (concat (keys include) (keys exclude)))
                     acc  (-> acc
                              (pop-entry-from-vs-queue vs-url dep-system-url)
                              (ensure-deps-processed deps vs-idx concepts-map)
                              (collect-mode-acc vs-url dep-system-url include concepts-map vs-idx :include)
                              (collect-mode-acc vs-url dep-system-url exclude concepts-map vs-idx :exclude)
                              (push-include-exclude->vs-idx vs-url dep-system-url concepts-map vs-idx))]
                 acc))
             acc
             (get-in acc [:refs-queue vs-url])))


(defn all-vs-nested-refs->vs-idx [concepts-map vs-idx nested-vs-refs-queue]
  (loop [acc {:refs-queue nested-vs-refs-queue
              :vs-idx-acc {}}]
    (let [res-acc (refs-in-vs->vs-idx acc (ffirst (:refs-queue acc)) vs-idx concepts-map)]
      (if (seq (:refs-queue res-acc))
        (recur res-acc)
        (:vs-idx-acc res-acc)))))


(defn push-vs-url-into-concepts-map [concepts-map system code vs-url]
  (update-in concepts-map
             [system code :valueset]
             (fnil conj #{})
             vs-url))


(defn reduce-vs-idx-into-concepts-map [concepts-map vs-idx]
  (reduce-kv (fn [acc vs-url vs-cs-idx]
               (reduce-kv (fn [acc sys codes]
                            (reduce (fn [acc code]
                                      (push-vs-url-into-concepts-map acc sys code vs-url))
                                    acc
                                    codes))
                          acc
                          vs-cs-idx))
             concepts-map
             vs-idx))


(defn all-vs-nested-refs->concepts-map [concepts-map vs-idx nested-vs-refs-queue]
  (let [new-vs-idx-entries (all-vs-nested-refs->vs-idx concepts-map vs-idx nested-vs-refs-queue)]
    (reduce-vs-idx-into-concepts-map concepts-map new-vs-idx-entries)))


(defn denormalize-into-concepts [ztx valuesets concepts-map']
  (let [{:keys [concepts-map
                vs-idx
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
                            (update-in [:vs-idx (:url vs) system]
                                       (fnil conj #{})
                                       (:code concept)))
                        acc))
                    acc
                    concepts))
                (-> acc
                    (update :nested-vs-refs-queue push-entries-to-vs-queue include-depends :include)
                    (update :nested-vs-refs-queue push-entries-to-vs-queue exclude-depends :exclude))
                (select-keys (:concepts-map acc) systems))))
          {:concepts-map         concepts-map'
           :vs-idx               {}
           :nested-vs-refs-queue {}}
          valuesets)]
    (all-vs-nested-refs->concepts-map concepts-map vs-idx nested-vs-refs-queue)))


(defn denormalize-value-sets-into-concepts [ztx]
  (swap! ztx update-in [:fhir/inter "Concept"]
         (partial denormalize-into-concepts
                  ztx (vals (get-in @ztx [:fhir/inter "ValueSet"])))))
