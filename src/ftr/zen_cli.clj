(ns ftr.zen-cli
  (:gen-class)
  (:require [zen.cli]
            [ftr.zen-package]
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


(def cfg (-> zen.cli/cfg
             (update :subcommands conj {:description "Builds FTR from this zen fhir package"
                                        :command "build-ftr"
                                        :runs (fn [args]
                                                (let [ztx (zen.cli/load-ztx args)]
                                                  (zen.cli/load-used-namespaces ztx #{})
                                                  (ftr.zen-package/build-ftr ztx)))})
             (update :subcommands conj {:description "Builds FTR from this zen fhir package"
                                        :command "get-ftr-index-info"
                                        :runs ftr.zen-package/get-ftr-index-info})))


(defn -main
  [& args]
  (let [og-read-ns zen.core/read-ns
        og-validate zen.core/validate]
    (with-redefs
      [zen.core/read-ns (fn [ztx zen-ns]
                          (og-read-ns ztx zen-ns)
                          (ftr.zen-package/build-ftr-index ztx))
       zen.core/validate (fn [ztx symbols data]
                           (-> (ftr.zen-package/validate ztx symbols data)
                               (select-keys [:errors :warnings :effects])))]
      (if (seq args)
        (cli-matic.core/run-cmd args cfg)
        (zen.cli/repl cfg)))))
