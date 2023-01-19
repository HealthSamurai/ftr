(ns ftr.extraction.core
  (:require [ftr.extraction.flat-table :as flat-table]
            [ftr.extraction.ig.core]
            [ftr.extraction.snomed]
            [ftr.extraction.serialized-objects-array]
            [ftr.extraction.ftr]))

(defn extract [cfg]
  (let [{:keys [source-type source-url source-urls extractor-options]} cfg
        extractor-cfg (assoc extractor-options
                             :source-url source-url
                             :node-modules-folder source-url
                             :source-urls source-urls)]
    (condp = source-type
      :flat-table (flat-table/import-from-cfg extractor-cfg)
      :ig (ftr.extraction.ig.core/import-from-cfg extractor-cfg)
      :igs (ftr.extraction.ig.core/import-from-cfg extractor-cfg)
      :snomed (ftr.extraction.snomed/import-from-cfg extractor-cfg)
      :serialized-objects-array (ftr.extraction.serialized-objects-array/import-from-cfg extractor-cfg)
      :ftr (ftr.extraction.ftr/import-from-cfg extractor-cfg))))
