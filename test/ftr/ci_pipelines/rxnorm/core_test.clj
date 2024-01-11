(ns ftr.ci-pipelines.rxnorm.core-test
  (:require
   [ftr.utils.core]
   [ftr.ci-pipelines.rxnorm.core :as sut]
   [ftr.ci-pipelines.rxnorm.test-utils :as utils]
   [clojure.test         :as t]
   [matcho.core          :as matcho]
   [clojure.java.io      :as io]
   [ftr.test-utils]
   [ftr.utils.unifn.core :as u]))

(defn prepare-test-env []
  (let [extraction-path (str (:working-dir-path sut/config-defaults) \/ "uncompessed-rxnorm-bundle")]
    (ftr.utils.core/rmrf (:ftr-path sut/config-defaults))
    (doseq [[to content] utils/bundle-entries]
      (io/make-parents (str extraction-path \/ to))
      (spit (str extraction-path \/ to) content))))

(def test-pipeline
  [::sut/build-ftr-cfg
   :ftr.core/apply-cfg])

(defn pipeline [args]
  (let [cfg (-> (merge sut/config-defaults args))]
    (u/*apply test-pipeline cfg)))

(t/deftest extract-rxnorm-version-test
  (prepare-test-env)
  (matcho/match
    (->>
      (io/file "/tmp/rxnorm_work_dir/uncompessed-rxnorm-bundle")
      (ftr.ci-pipelines.rxnorm.core/extract-rxnorm-version))
    "10022023"))

(t/deftest core-test
  (prepare-test-env)

  (pipeline {:extract-destination "/tmp/rxnorm_work_dir/uncompessed-rxnorm-bundle"
             :rxnorm-version      "20070101"})

  (matcho/match
    (ftr.test-utils/fs-tree->tree-map (:ftr-path sut/config-defaults))
    {"rxnorm"
     {"vs"
      {"http:--www.nlm.nih.gov-research-umls-rxnorm-valueset"
       {"tag.prod.ndjson.gz" {},
        "tf.fb592cc629824d83c68682dbec3e0611b23eb5ccb770a854cdda1aebcc4fb887.ndjson.gz"
        {}}},
      "tags" {"prod.ndjson.gz" {}, "prod.hash" {}}}}

)

  (matcho/match
   (ftr.utils.core/parse-ndjson-gz
    (format "%s/rxnorm/tags/prod.ndjson.gz" (:ftr-path sut/config-defaults)))
   [{:hash
     "fb592cc629824d83c68682dbec3e0611b23eb5ccb770a854cdda1aebcc4fb887",
     :name "rxnorm.http:--www.nlm.nih.gov-research-umls-rxnorm-valueset"}
    nil])

  (matcho/match
    (ftr.utils.core/parse-ndjson-gz
      (format "%s/rxnorm/vs/http:--www.nlm.nih.gov-research-umls-rxnorm-valueset/tf.fb592cc629824d83c68682dbec3e0611b23eb5ccb770a854cdda1aebcc4fb887.ndjson.gz"
              (:ftr-path sut/config-defaults)))
    [{:publisher    "National Library of Medicine (NLM)"
      :content      "not-present"
      :name         "RxNorm"
      :resourceType "CodeSystem"
      :status       "active"
      :id           "rxnorm-cs"
      :valueSet     "http://www.nlm.nih.gov/research/umls/rxnorm/valueset"
      :url          "http://www.nlm.nih.gov/research/umls/rxnorm"
      :version      "20070101"}
     {:compose      {:include [{:system "http://www.nlm.nih.gov/research/umls/rxnorm"}]}
      :id           "rxnorm-vs"
      :name         "RxNorm"
      :resourceType "ValueSet"
      :status       "active"
      :url          "http://www.nlm.nih.gov/research/umls/rxnorm/valueset"
      :version      "20070101"}
     {:code         "1337"
      :display      "tablets"
      :property     {:suppressible-flag "O" :tty "SCD"}
      :resourceType "Concept"
      :system       "http://www.nlm.nih.gov/research/umls/rxnorm"
      :valueset     ["http://www.nlm.nih.gov/research/umls/rxnorm/valueset"]}
     {:code         "2665426"
      :display      "0.8 ML adalimumab-adaz 50 MG/ML Prefilled Syringe [Hyrimoz]"
      :property
      {:tty "SCD"
       :other-display
       ["Hyrimoz 40 MG in 0.8 ML Prefilled Syringe"
        "0.8 ML Hyrimoz 50 MG/ML Prefilled Syringe"
        "Hyrimoz 40 MG per 0.8 ML Prefilled Syringe"]
       :suppressible-flag "O"}
      :resourceType "Concept"
      :system       "http://www.nlm.nih.gov/research/umls/rxnorm"
      :valueset     ["http://www.nlm.nih.gov/research/umls/rxnorm/valueset"]}
     {:code         "2665428"
      :display      "cholecalciferol 0.075 MG / folic acid 1 MG Oral Capsule"
      :property
      {:tty "SCD"
       :other-display
       ["vitamin D 3 75 MCG (3000 UNT) / folic acid 1 MG Oral Capsule"
        "vitamin D 3 75 MCG (3000 UNT) / folic acid 1 MG Oral Capsule"
        "cholecalciferol 0.075 MG / folate 1 MG Oral Capsule"]
       :suppressible-flag "N"}
      :resourceType "Concept"
      :system       "http://www.nlm.nih.gov/research/umls/rxnorm"
      :valueset     ["http://www.nlm.nih.gov/research/umls/rxnorm/valueset"]}]))
