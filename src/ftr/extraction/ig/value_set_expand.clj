(ns ftr.extraction.ig.value-set-expand
  (:require
   [zen.utils]
   [clojure.set :as set]
   [clojure.string :as str]))


(defn vs-compose-concept-fn [value-set system version concepts]
  (when (seq concepts)
    (let [concept-codes (into #{}
                              (comp (map :code)
                                    (map keyword))
                              concepts)
          system-kw (keyword system)]
      (fn vs-compose-concept [concept]
        #_(and (= system-kw (:zen.fhir/system-kw concept))
               (contains? concept-codes (:zen.fhir/code-kw concept)))
        #_"NOTE: we're not checking sys since concepts from other sys should not be sent into this fn"
        (contains? concept-codes (:zen.fhir/code-kw concept))))))


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
  (let [in-values (into #{}
                        (map str/trim)
                        (str/split (or (:value filter) "") #","))]
    (fn in-op [concept]
      (contains? in-values
                 (get (:property concept) (:property filter))))))


(defmethod filter-op "not-in" [_value-set _system _version filter]
  (let [in-values (into #{}
                        (map str/trim)
                        (str/split (or (:value filter) "") #","))]
    (fn not-in-op [concept]
      (not (contains? in-values
                      (get (:property concept) (:property filter)))))))


(defmethod filter-op "exists" [_value-set _system _version filter]
  (if (= "false" (some-> (:value filter) str/lower-case str/trim))
    (fn not-exists-op [concept] (nil? (get (:property concept) (:property filter))))
    (fn exists-op [concept] (some? (get (:property concept) (:property filter))))))


(defmethod filter-op "is-a" [_value-set _system _version filter]
  (let [is-a-keyword (keyword (:value filter))]
    (fn is-a-op [concept]
      (or (= (:zen.fhir/code-kw concept) is-a-keyword)
          (contains? (:zen.fhir/parents concept) is-a-keyword)))))


(defmethod filter-op "descendent-of" [_value-set _system _version filter]
  (let [is-a-keyword (keyword (:value filter))]
    (fn descendatnd-of-op [concept]
      (contains? (:zen.fhir/parents concept) is-a-keyword))))


(defmethod filter-op "is-not-a" [_value-set _system _version filter]
  (let [is-a-keyword (keyword (:value filter))]
    (fn is-not-a-op [concept]
      (not (or (= (:zen.fhir/code-kw concept) is-a-keyword)
               (contains? (:zen.fhir/parents concept) is-a-keyword))))))


(defmethod filter-op "regex" [_value-set _system _version filter]
  (let [re-pat (re-pattern (:value filter))]
    (fn regex-op [concept]
      (when-let [prop (get (:property concept) (:property filter))]
        (re-matches re-pat (str prop))))))


(defn vs-compose-filter-fn [value-set system version filters]
  (when (seq filters)
    (let [filter-fns (->> filters
                          (map #(filter-op value-set system version %)))

          filter-fn (if (= 1 (count filter-fns))
                      (first filter-fns) #_"NOTE: eliminate every-pred to simplify stacktraces"
                      (apply every-pred filter-fns))

          system-kw (keyword system)]
      (fn compose-filter [concept]
        #_(and (= system-kw (:zen.fhir/system-kw concept))
               (filter-fn concept))
        #_"NOTE: we're not checking sys since concepts from other sys should not be sent into this fn"
        (filter-fn concept)))))


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


(defn push-concept-into-vs-idx [vs-idx concept]
  (update vs-idx
          (:system concept)
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


(defn vs-selected-system-intersection->vs-idx [acc concepts-map expansion-index vs-url sys vs-urls checks
                                               & {:keys [any-system? mode]}]
  (let [acc-key (get-acc-key any-system? mode)]
    (if-let [concepts
             (cond
               (seq vs-urls)
               (some->> vs-urls
                        (map (fn [dep-vs-url]
                               (get-in acc [:vs-idx-acc dep-vs-url sys] #{})))
                        (apply set/intersection)
                        seq
                        (map (fn [code] [code (get-in concepts-map [sys code])]))
                        (into {}))

               sys
               (get concepts-map sys)

               :else
               (throw (ex-info "ValueSet or system should be present" {:vs-url vs-url, :sys sys, :deps vs-urls})))]
      (if-let [check-fns (seq (:pred-fns checks))]
        (let [check-fn (if (= 1 (count check-fns))
                         (first check-fns)
                         (fn [concept] (some #(% concept) check-fns)))]
          (update-in acc [acc-key vs-url]
                     (fn [vs-idx-acc]
                       (transduce (filter (fn [[concept-code concept]] (and #_(not-any? #(% concept) exclude-check-fns) #_"TODO: instead of building exclude idx maybe check exclude on building include idx?"
                                                                            (or (when (= :include mode)
                                                                                  #_"NOTE: this when can not tested, because if expansion is included without checking exclude."
                                                                                  #_"NOTE: Without the 'when exclude gets a codes from expansion and it forbids to include these values,"
                                                                                  #_"NOTE: but these values are already included, thus this effect is not observable form outside"
                                                                                  (-> expansion-index
                                                                                      (get (:system concept))
                                                                                      (get concept-code)))
                                                                                (check-fn concept)))))
                                  (completing (fn [acc [_concept-code concept]] (push-concept-into-vs-idx acc concept)))
                                  (or vs-idx-acc {})
                                  concepts))))
        (if (:allow-any-concept checks)
          (update-in acc [acc-key vs-url sys] (fnil into #{}) (keys concepts))
          (throw (ex-info "must be either predicate fn or whole system allow"
                          {:vs-url vs-url, :sys sys, :deps vs-urls}))))
      (update-in acc [acc-key vs-url] #(or % {})))))


(defn select-all-dep-systems [vs-idx-acc deps-vs-urls]
  (mapcat #(keys (get vs-idx-acc %))
          deps-vs-urls))


(defn vs-selected-systems->mode-acc [acc concepts-map expansion-index vs-url dep-system-url vs-urls checks mode]
  (let [any-system? (= ::any-system dep-system-url)
        selected-systems (if any-system?
                           (select-all-dep-systems (:vs-idx-acc acc) vs-urls)
                           [dep-system-url])]
    (reduce (fn [acc sys]
              (vs-selected-system-intersection->vs-idx acc concepts-map expansion-index vs-url sys vs-urls checks
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


(defn collect-mode-acc [acc concepts-map expansion-index vs-url system compose-els mode]
  (reduce-kv (fn [acc vs-urls checks]
               (vs-selected-systems->mode-acc acc concepts-map expansion-index vs-url system vs-urls checks mode))
             acc
             compose-els))


(defn push-include-exclude->vs-idx [acc vs-url dep-system-url]
  (let [any-system?     (= ::any-system dep-system-url)
        incl-acc-key    (get-acc-key any-system? :include)
        vs-sys-idx-acc  (get-in acc [:vs-idx-acc vs-url] {})
        sys-exclude-idx (get-in acc [:any-sys-exclude-acc vs-url])
        any-exclude-idx (get-in acc [:exclude-acc vs-url])
        exclude-contains-sys? (fn [system]
                                (or (get-in sys-exclude-idx [system])
                                    (get-in any-exclude-idx [system])))
        exclude-remove  (when (or (seq sys-exclude-idx) (seq any-exclude-idx))
                          (fn [system code]
                            (or (get-in sys-exclude-idx [system code])
                                (get-in any-exclude-idx [system code]))))

        new-vs-sys-idx #_"TODO: refactor"
        (if any-system?
          (if exclude-remove
            (reduce-kv (fn [acc sys concepts]
                         (if (exclude-contains-sys? sys)
                           (update-if-some-result acc sys
                                                  (fn [idx-concepts]
                                                    (not-empty
                                                      (into (or idx-concepts #{})
                                                            (remove #(exclude-remove sys %))
                                                            concepts))))
                           (update acc sys
                                   #(if (some? %1)
                                      (into %1 %2)
                                      %2)
                                   concepts)))
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
            (if (and exclude-remove (exclude-contains-sys? dep-system-url))
              (fn [idx-concepts new-concepts]
                (not-empty (into (or idx-concepts #{})
                                 (remove #(exclude-remove dep-system-url %))
                                 new-concepts)))
              #(if (some? %1)
                 (into %1 %2)
                 %2))
            (get-in acc [incl-acc-key vs-url dep-system-url])))]
    (assoc-in acc [:vs-idx-acc vs-url] new-vs-sys-idx)))


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

            acc (collect-mode-acc acc concepts-map expansion-index vs-url ::any-system (:any-system exclude) :exclude)

            need-to-process-all-excludes? (:any-system include)

            acc (if need-to-process-all-excludes? #_"NOTE: can process not all, but only ones that will be used in any-system include"
                  (reduce-kv (fn [acc dep-system-url exclude]
                               (-> acc
                                   (collect-mode-acc concepts-map expansion-index vs-url dep-system-url exclude :exclude)
                                   (push-include-exclude->vs-idx vs-url dep-system-url)))
                             acc
                             (:systems exclude))
                  acc)

            acc (reduce-kv (fn [acc dep-system-url include]
                             (-> acc
                                 (cond-> (not need-to-process-all-excludes?)
                                   (collect-mode-acc concepts-map expansion-index vs-url dep-system-url
                                                     (get-in exclude [:systems dep-system-url])
                                                     :exclude))
                                 (collect-mode-acc concepts-map expansion-index vs-url dep-system-url include :include)
                                 (push-include-exclude->vs-idx vs-url dep-system-url)))
                           acc
                           (:systems include))

            acc (-> acc
                    (collect-mode-acc concepts-map expansion-index vs-url ::any-system (:any-system include) :include)
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
