(ns ftr.zen-cli
  (:gen-class)
  (:require [zen.cli]
            [ftr.zen-package]
            [zen.core]))

(def cfg (-> zen.cli/cfg
             (update :subcommands conj {:description "Builds FTR from this zen fhir package"
                                        :command "build-ftr"
                                        :runs ftr.zen-package/build-ftr})))


(defn -main
  [& args]
  (let [og-read-ns zen.core/read-ns
        og-validate zen.core/validate]
    (with-redefs
      [zen.core/read-ns (fn [ztx zen-ns]
                          (og-read-ns ztx zen-ns)
                          (ftr.zen-package/ftr->memory-cache ztx))
       zen.core/validate (fn [ztx symbols data]
                           (-> (ftr.zen-package/validate ztx symbols data)
                               (select-keys [:errors :warnings :effects])))]
      (if (seq args)
        (cli-matic.core/run-cmd args cfg)
        (zen.cli/repl cfg)))))
