(ns ftr.ci-pipelines.loinc.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [ftr.ci-pipelines.loinc.core :as sut]
            [ftr.test-utils :as test-utils]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [matcho.core :as matcho]))


(def bundle-entries
    [["LoincTableCore/LoincTableCore.csv" (slurp "test/fixture/loinc/loinc_table_core.csv")]
     ["AccessoryFiles/PartFile/Part.csv" (slurp "test/fixture/loinc/part.csv")]
     ["AccessoryFiles/PartFile/LoincPartLink_Primary.csv" (slurp "test/fixture/loinc/loinc_part_link_primary.csv")]
     ["AccessoryFiles/ComponentHierarchyBySystem/ComponentHierarchyBySystem.csv" (slurp "test/fixture/loinc/component_hierarchy_by_system.csv")]
     ["AccessoryFiles/LinguisticVariants/frCA8LinguisticVariant.csv" (slurp "test/fixture/loinc/fr_ca_8_linguistic_variant.csv")]
     ["AccessoryFiles/LinguisticVariants/frFR18LinguisticVariant.csv" (slurp "test/fixture/loinc/fr_fr_18_linguistic_variant.csv")]])


(defn prepare-test-env []
  (let [extraction-path (str (:working-dir-path sut/config-defaults) \/ "uncompessed-loinc-bundle")]
    (ftr.utils.core/rmrf (:ftr-path sut/config-defaults))
    (doseq [[to content] bundle-entries]
      (io/make-parents (str extraction-path \/ to))
      (spit (str extraction-path \/ to) content))))


(def test-pipeline
  [::sut/build-ftr-cfg
   :ftr.core/apply-cfg])


(defn pipeline [args]
  (let [cfg (-> (merge sut/config-defaults args))]
    (u/*apply test-pipeline cfg)))

(t/deftest extractor-test
  (prepare-test-env)

  (pipeline {:extract-destination                   "/tmp/loinc_work_dir/uncompessed-loinc-bundle"
             :join-original-language-as-designation true
             :langs                                 [{:id "8", :lang "fr", :country "CA"}
                                                     {:id "18", :lang "fr", :country "FR"}]})

  (matcho/match
    (test-utils/fs-tree->tree-map (:ftr-path sut/config-defaults))
    {"loinc"
     {"vs"
      {"http:--loinc.org-vs"
       {"tf.b42fe5da59a2fd08799e2f432833f73f2e063fdc76159fa31d511b479438b040.ndjson.gz" {}
        "tag.prod.ndjson.gz" {}}}
      "tags" {"prod.ndjson.gz" {} "prod.hash" {}}}})

  (matcho/match
    (ftr.utils.core/parse-ndjson-gz
      (format "%s/loinc/tags/prod.ndjson.gz" (:ftr-path sut/config-defaults)))
    [{:hash "b42fe5da59a2fd08799e2f432833f73f2e063fdc76159fa31d511b479438b040"
      :name "loinc.http:--loinc.org-vs"}
     nil?])

  (matcho/match
    (ftr.utils.core/parse-ndjson-gz
      (format "%s/loinc/vs/http:--loinc.org-vs/tf.b42fe5da59a2fd08799e2f432833f73f2e063fdc76159fa31d511b479438b040.ndjson.gz" (:ftr-path sut/config-defaults)))
    [{}
     {}
     {:id          "http:--loinc.org-http:--loinc.org-vs-14575-5"
      :code        "14575-5"
      :designation [{:language "en"
                     :value "Blood group antibody investigation [Interpretation] in Plasma or RBC"}
                    {:language "fr-CA"
                     :value
                     "Recherche d'anticorps des groupes sanguins:Impression/interprétation d'étude:Temps ponctuel:Plasma/GR:Nominal:"}
                    {:language "fr-FR"
                     :value
                     "Anticorps irréguliers (RAI) [Interprétation] Plasma/Érythrocytes ; Résultat nominal"}
                    nil?]}
     {:id "http:--loinc.org-http:--loinc.org-vs-19146-0" :code "19146-0"}
     {:id          "http:--loinc.org-http:--loinc.org-vs-43119-7"
      :code        "43119-7"
      :designation [{:language "en"
                     :value "Extractable nuclear Ab panel - Serum"}
                    {:language "fr-CA"
                     :value
                     "Recherche d'Ac nucléaires solubles:-:Temps ponctuel:Sérum:-:"}
                    {:language "fr-FR"
                     :value
                     "Antigènes nucléaires solubles anticorps panel [-] Sérum ; -"}
                    nil?]}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP36399-1" :code "LP36399-1"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP422758-5" :code "LP422758-5"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP422835-1" :code "LP422835-1"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP67165-8" :code "LP67165-8"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP6769-6" :code "LP6769-6"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP6813-2" :code "LP6813-2"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP6819-9" :code "LP6819-9"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP6960-1" :code "LP6960-1"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP7481-7" :code "LP7481-7"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP7567-3" :code "LP7567-3"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP7747-1" :code "LP7747-1"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP7749-7" :code "LP7749-7"}
     {:id "http:--loinc.org-http:--loinc.org-vs-LP7750-5" :code "LP7750-5"}
     nil?]))
