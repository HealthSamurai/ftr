(ns ftr.zen-cli
  (:gen-class)
  (:require [zen.cli]
            [ftr.zen-package]
            [clojure.pprint]
            [zen.core]
            [zen.store]
            [cli-matic.core]))


(defmethod zen.v2-validation/compile-key :zen.fhir/value-set
  [_ _ztx value-set]
  {:rule
   (fn [vtx data opts]
     (if (= :required (:strength value-set))
       (let [schemas-stack (->> (:schema vtx) (partition-by #{:confirms}) (take-nth 2) (map first))]
         (update-in vtx [:fx :zen.fhir/value-set :value-sets] conj
                    {:schemas   schemas-stack
                     :path      (:path vtx)
                     :data      data
                     :value-set (:symbol value-set)
                     :strength  (:strength value-set)}))
       vtx))})


(def cfg (-> zen.cli/commands
             (assoc "build-ftr" (fn [opts]
                                  (let [ztx (zen.cli/load-ztx opts)]
                                    (zen.cli/load-used-namespaces ztx #{})
                                    (ftr.zen-package/build-ftr ztx {:enable-logging? true}))))
             (assoc "get-ftr-index-info" ftr.zen-package/get-ftr-index-info)))


(defn -main
  [& [cmd-name & args]]
  (let [og-read-ns zen.core/read-ns]
    (with-redefs
      [zen.core/read-ns (fn [ztx zen-ns]
                          (og-read-ns ztx zen-ns)
                          (ftr.zen-package/build-complete-ftr-index ztx))
       zen.core/validate (fn [ztx symbols data]
                           (-> (ftr.zen-package/validate ztx symbols data)
                               (select-keys [:errors :warnings :effects])))]
      (if (some? cmd-name)
        (clojure.pprint/pprint (zen.cli/cmd cfg cmd-name args))
        (zen.cli/repl cfg))
      (System/exit 0))))
