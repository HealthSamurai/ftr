(ns ftr.zen-cli
  (:gen-class)
  (:require [zen.cli]
            [ftr.zen-package]
            [clojure.pprint]
            [zen.core]
            [zen.store]
            [zen.v2-validation]))


(defmethod zen.v2-validation/compile-key :zen.fhir/value-set
  [_ _ztx value-set]
  {:rule
   (fn [vtx data _opts]
     (if (= :required (:strength value-set))
       (let [schemas-stack (->> (:schema vtx) (partition-by #{:confirms}) (take-nth 2) (map first))]
         (update-in vtx [:fx :zen.fhir/value-set :value-sets] conj
                    {:schemas   schemas-stack
                     :path      (:path vtx)
                     :data      data
                     :value-set (:symbol value-set)
                     :strength  (:strength value-set)}))
       vtx))})


(defmethod zen.cli/command 'extended-zen-cli/build-ftr [_ _ opts]
  (let [ztx (zen.cli/load-ztx opts)]
    (zen.cli/load-used-namespaces ztx #{})
    (ftr.zen-package/build-ftr ztx {:enable-logging? true})))


(defmethod zen.cli/command 'extended-zen-cli/get-ftr-index-info [_ _ opts]
  (ftr.zen-package/get-ftr-index-info opts))


(defn -main
  [& args]
  (let [og-read-ns zen.core/read-ns
        ztx (zen.core/new-context)
        _ (zen.core/read-ns ztx 'extended-zen-cli)]
    (with-redefs
      [zen.core/read-ns (fn [ztx zen-ns]
                          (let [read-ns-result (og-read-ns ztx zen-ns)]
                            (ftr.zen-package/build-complete-ftr-index ztx)
                            read-ns-result))
       zen.core/validate (fn [ztx symbols data]
                           (-> (ftr.zen-package/validate ztx symbols data)
                               (select-keys [:errors :warnings :effects])))]
      (clojure.pprint/pprint (zen.cli/cli ztx 'extended-zen-cli/config args {:prompt-fn
                                                                             #(do (print "ftr> ")
                                                                                  (flush))}))
      (System/exit 0))))
