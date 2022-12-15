(ns ftr.extraction.ig.value-set-expand
  (:require
   [zen.utils]
   [clojure.set :as set]
   [clojure.string :as str]))


(defn vs-compose-concept-fn [value-set system version concepts]
  (when (seq concepts)
    (let [concept-codes (into #{} (map :code) concepts)]
      (fn vs-compose-concept [concept]
        (and (= system (:system concept))
             (contains? concept-codes (:code concept)))))))


(defmulti filter-op
  (fn [_value-set _system _version filter]
    (:op filter)))


(defmethod filter-op :default [_value-set _system _version _filter]
  (constantly false))


(defmethod filter-op "=" [_value-set _system _version filter]
  (fn eq-op [concept]
    (= (get (:property concept) (:property filter))
       (:value filter))))


(defmethod filter-op "in" [_value-set _system _version filter]
  (fn in-op [concept]
    (get (into #{} (map str/trim) (str/split (or (:value filter) "") #","))
         (get (:property concept) (:property filter)))))


(defmethod filter-op "not-in" [_value-set _system _version filter]
  (fn not-in-op [concept]
    (not (get (into #{} (map str/trim) (str/split (or (:value filter) "") #","))
              (get (:property concept) (:property filter))))))


(defmethod filter-op "exists" [_value-set _system _version filter]
  (if (= "false" (some-> (:value filter) str/lower-case str/trim))
    (fn not-exists-op [concept] (nil? (get (:property concept) (:property filter))))
    (fn exists-op [concept] (some? (get (:property concept) (:property filter))))))


(defmethod filter-op "is-a" [_value-set _system _version filter]
  (fn is-a-op [concept]
    (or (= (:code concept) (:value filter))
        (contains? (:zen.fhir/parents concept) (:value filter)))))


(defmethod filter-op "descendent-of" [_value-set _system _version filter]
  (fn descendatnd-of-op [concept]
    (contains? (set (:hierarchy concept)) (:value filter))))


(defmethod filter-op "is-not-a" [_value-set _system _version filter]
  (fn is-not-a-op [concept] ;; TODO: not sure this is correct impl by spec
    (and (not (contains? (set (:hierarchy concept)) (:value filter)))
         (not= (:code concept) (:value filter)))))


(defmethod filter-op "regex" [_value-set _system _version filter]
  (fn regex-op [concept]
    (when-let [prop (get (:property concept) (:property filter))]
      (re-matches (re-pattern (:value filter))
                  (str prop)))))


(defn vs-compose-filter-fn [value-set system version filters]
  (when (seq filters)
    (let [filter-fn (->> filters
                         (map #(filter-op value-set system version %))
                         (apply every-pred))]
      (fn compose-filter [concept]
        (and (= system (:system concept))
             (filter-fn concept))))))


(declare compose)


(defn check-concept-in-compose-el-fn [value-set compose-el]
  (let [code-system-pred
        (or (vs-compose-concept-fn value-set
                                   (:system compose-el)
                                   (:version compose-el)
                                   (:concept compose-el))
            (vs-compose-filter-fn value-set
                                  (:system compose-el)
                                  (:version compose-el)
                                  (:filter compose-el)))]
    (some-> {:check-fn   code-system-pred
             :system     (:system compose-el)
             :depends-on (:valueSet compose-el)}
            zen.utils/strip-nils
            not-empty
            (assoc :vs-url (:url value-set)))))


(defn vs-expansion-index [vs] #_"TODO: support recursive expansion :contains"
  (when (get-in vs [:expansion :contains])
    (let [expansion-contains (get-in vs [:expansion :contains])
          full-expansion?    (and (= (count expansion-contains) (get-in vs [:expansion :total]))
                                  (empty? (get-in vs [:expansion :parameter])))
          concepts-index (persistent!
                           (reduce (fn [acc concept]
                                     (assoc! acc (:system concept)
                                             (if-let [sys-idx (get acc (:system concept))]
                                               (conj sys-idx (:code concept))
                                               #{(:code concept)})))
                                   (transient {})
                                   expansion-contains))]
      {:concepts-index concepts-index
       :full?    full-expansion?})))


(defn compose [vs]
  (let [{full-expansion? :full?
         vs-concepts-index :concepts-index}
        (vs-expansion-index vs)

        includes (some->> (get-in vs [:compose :include])
                          (keep (partial check-concept-in-compose-el-fn vs))
                          not-empty)

        excludes (some->> (get-in vs [:compose :exclude])
                          (keep (partial check-concept-in-compose-el-fn vs))
                          not-empty)

        #_#_systems (into #{}
                          (mapcat (comp not-empty :system))
                          (concat includes excludes))]

    {#_#_:systems     (not-empty systems)
     :vs-url          (:url vs)
     :full-expansion? full-expansion?
     :expansion-index vs-concepts-index
     :includes        includes
     :excludes        excludes}))


(defn push-compose-el-to-vs-queue [vs-queue el-type {:keys [depends-on system check-fn]}]
  (let [has-dependencies?  (seq depends-on)
        allow-any-concept? (nil? check-fn)
        has-concept-check? (some? check-fn)
        any-system?        (nil? system)
        depends-on         (vec depends-on)

        _ (when (not has-concept-check?)
            (assert (or (some? system) has-dependencies?)
                    "check fn may be missing only when depending on another value set or there's a system"))
        _ (assert (or (not any-system?) has-dependencies?)
                  "system may be missing only when depending on another value set")

        queue-path (concat [el-type]
                           (if any-system?
                             [:any-system]
                             [:systems system])
                           (if allow-any-concept?
                             [depends-on :allow-any-concept]
                             [depends-on :pred-fns]))]
    (cond-> vs-queue
      allow-any-concept?
      (assoc-in queue-path true)

      has-concept-check?
      (update-in queue-path conj check-fn)

      has-dependencies?
      (update :deps (fnil into #{}) depends-on))))


(defn push-compose-els-to-vs-queue [vs-queue el-type els]
  (reduce #(push-compose-el-to-vs-queue %1 el-type %2)
          vs-queue
          els))


(defn push-entries-to-vs-queue [vs-queue {:keys [full-expansion? expansion-index includes excludes]}]
  (-> vs-queue
      (push-compose-els-to-vs-queue :include includes)
      (push-compose-els-to-vs-queue :exclude excludes)
      (zen.utils/assoc-some :full-expansion? full-expansion?
                            :expansion-index expansion-index)))


(defn pop-entry-from-vs-queue [acc vs-url]
  (update acc :vs-queue dissoc vs-url))


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


(defn update-if-some-result [m k f & args]
  (if-let [result (apply f (get m k) args)]
    (assoc m k result)
    m))


(defn vs-selected-system-intersection->vs-idx [acc concepts-map vs-url sys vs-urls checks & {:keys [any-system? mode]}]
  (if-let [concepts
           (cond
             (seq vs-urls)
             (some->> vs-urls
                      (keep (fn [dep-vs-url]
                              (get-in acc [:vs-idx-acc dep-vs-url sys])))
                      seq
                      (apply set/intersection)
                      (map (fn [code] [code (get-in concepts-map [sys code])]))
                      (into {}))

             sys
             (get concepts-map sys)

             :else
             (throw (ex-info "ValueSet or system should be present" {:vs-url vs-url, :sys sys, :deps vs-urls})))]
    (let [acc-key (get-acc-key any-system? mode)]
      (if-let [check-fns (seq (:pred-fns checks))]
        (update acc acc-key
                (fn [vs-idx-acc]
                  (transduce (filter (fn [[concept-code concept]] (and #_(not-any? #(% concept) exclude-check-fns)
                                                                       (or (get-in acc [:vs-idx-acc vs-url concept-code])
                                                                           (some #(% concept) check-fns)))))
                             (completing (fn [acc [_concept-code concept]] (push-concept-into-vs-idx acc vs-url concept)))
                             vs-idx-acc
                             concepts)))
        (if (:allow-any-concept checks)
          (update-in acc [acc-key vs-url sys] (fnil into #{}) (keys concepts))
          (throw (ex-info "must be either predicate fn or whole system allow"
                          {:vs-url vs-url, :sys sys, :deps vs-urls})))))
    acc))


(defn select-all-dep-systems [vs-idx-acc deps-vs-urls]
  (mapcat #(keys (get vs-idx-acc %))
          deps-vs-urls))


(defn vs-selected-systems->mode-acc [acc concepts-map vs-url dep-system-url vs-urls checks mode]
  (let [any-system? (= ::any-system dep-system-url)
        selected-systems (if any-system?
                           (select-all-dep-systems (:vs-idx-acc acc) vs-urls)
                           [dep-system-url])]
    (reduce (fn [acc sys]
              (vs-selected-system-intersection->vs-idx acc concepts-map vs-url sys vs-urls checks
                                                       {:any-system? any-system?
                                                        :mode mode}))
            acc
            selected-systems)))


(declare refs-in-vs->vs-idx)


(defn ensure-deps-processed [acc concepts-map vs-urls]
  (transduce (filter #(seq (get-in acc [:vs-queue %])))
             (completing #(refs-in-vs->vs-idx %1 concepts-map %2))
             acc
             vs-urls))


(defn collect-mode-acc [acc concepts-map vs-url system compose-els mode]
  (reduce-kv (fn [acc vs-urls checks]
               (vs-selected-systems->mode-acc acc concepts-map vs-url system vs-urls checks mode))
             acc
             compose-els))


(defn push-include-exclude->vs-idx [acc vs-url dep-system-url]
  (let [any-system?    (= ::any-system dep-system-url)
        incl-acc-key   (get-acc-key any-system? :include)
        #_#_excl-acc-key   (get-acc-key any-system? :exclude)
        vs-sys-idx-acc (get-in acc [:vs-idx-acc vs-url])
        #_#_exclude-idx    (get-in acc [excl-acc-key vs-url])
        exclude-filter nil #_(when (seq exclude-idx)
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
          (update-if-some-result
            vs-sys-idx-acc
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
    (cond-> acc
      (some? new-vs-sys-idx)
      (assoc-in [:vs-idx-acc vs-url] new-vs-sys-idx))))


(defn refs-in-vs->vs-idx [acc concepts-map vs-url]
  (let [{:keys [deps include exclude full-expansion? expansion-index]} (get-in acc [:vs-queue vs-url])

        acc (-> acc
                (pop-entry-from-vs-queue vs-url)
                (ensure-deps-processed concepts-map deps))]
    (if full-expansion?
      (update-in acc [:vs-idx-acc vs-url] #(merge-with into % expansion-index))
      (let [acc (cond-> acc
                  (seq expansion-index)
                  (update-in [:vs-idx-acc vs-url] #(merge-with into % expansion-index)))

            acc (reduce-kv (fn [acc dep-system-url include]
                             (-> acc
                                 (collect-mode-acc concepts-map vs-url dep-system-url include :include)
                                 (push-include-exclude->vs-idx vs-url dep-system-url)))
                           acc
                           (:systems include))

            acc (-> acc
                    (collect-mode-acc concepts-map vs-url ::any-system (:any-system include) :include)
                    (push-include-exclude->vs-idx vs-url ::any-system))]
        acc))))


(defn all-vs-nested-refs->vs-idx [concepts-map nested-vs-refs-queue]
  (loop [acc {:vs-queue nested-vs-refs-queue
              :vs-idx-acc {}}]
    (let [res-acc (refs-in-vs->vs-idx acc concepts-map (ffirst (:vs-queue acc)))]
      (if (seq (:vs-queue res-acc))
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


(defn build-valuesets-compose-idx [valuesets]
  (transduce
    (map compose)
    (completing (fn [queue {:as vs-comp-res :keys [vs-url]}]
                  (update queue vs-url push-entries-to-vs-queue vs-comp-res)))
    {}
    valuesets))


(defn denormalize-into-concepts [valuesets concepts-map]
  (let [nested-vs-refs-queue (build-valuesets-compose-idx valuesets)
        new-vs-idx-entries   (all-vs-nested-refs->vs-idx concepts-map nested-vs-refs-queue)]
    (reduce-vs-idx-into-concepts-map concepts-map new-vs-idx-entries)))


(defn denormalize-value-sets-into-concepts [ztx]
  (swap! ztx update-in [:fhir/inter "Concept"]
         (partial denormalize-into-concepts
                  (vals (get-in @ztx [:fhir/inter "ValueSet"])))))
