(ns ftr.ci-pipelines.snomed.core-test
  (:require [ftr.ci-pipelines.snomed.core :as sut]
            [clojure.test :as t]
            [matcho.core :as matcho]
            [clojure.string :as str]
            [ftr.utils.unifn.core :as u]
            [test-utils]))


(def not-empty-string?
  (every-pred string? (complement empty?)))


(t/deftest get-latest-snomed-download-url-test
  (matcho/match
    (u/*apply [::sut/get-latest-snomed-info!] sut/config-defaults)
    {:snomed-info
     {:snomed-zip-url not-empty-string?
      :version not-empty-string?
      :complete-download-url not-empty-string?}})

  (matcho/match
    (u/*apply [::sut/get-latest-snomed-info!] sut/config-defaults)
    {:snomed-info
     {:complete-download-url #(str/starts-with? % "https://uts-ws.nlm.nih.gov/download?url=https://download.nlm.nih.gov/mlb/utsauth/USExt/SnomedCT_USEditionRF2_PRODUCTION_")}}))


(t/deftest write-snomed-snapshot-terminology-folder-test
  (def zip-file "/tmp/write-snomed-snapshot-terminology-folder-test/test.zip")
  (def write-path "/tmp/write-snomed-snapshot-terminology-folder-test/")

  (test-utils/create-zip-archive-from-fs-tree
    {:SomethingThatLooksLikeSnomed_TESTEdition
     {:Full
      {:Terminology
       {:somejunk1.txt :file
        :somejunk2.txt :file}}
      :Snapshot
      {:Terminology
       {:sct2_Concept_Snapshot.txt                    :file
        :sct2_Description_Snapshot.txt                :file
        :sct2_Identifier_Snapshot.txt                 :file
        :sct2_RelationshipConcreteValues_Snapshot.txt :file
        :sct2_Relationship_Snapshot.txt               :file
        :sct2_StatedRelationship_Snapshot.txt         :file
        :sct2_TextDefinition_Snapshot.txt             :file
        :sct2_sRefset_OWLExpressionSnapshot           :file}
       :Refset
       {:somejunk1.txt :file
        :somejunk2.txt :file}}}}
    zip-file)

  (u/*apply [::sut/write-snomed-snapshot-terminology-folder!]
            {:extracted-snomed-out-dir write-path
             :snomed-info
             {:complete-download-url zip-file}})

  (t/is
    (= {"test.zip" {}
        "SomethingThatLooksLikeSnomed_TESTEdition"
        {"Snapshot"
         {"Terminology"
          {"sct2_TextDefinition_Snapshot.txt" {}
           "sct2_Description_Snapshot.txt" {}
           "sct2_Relationship_Snapshot.txt" {}
           "sct2_Concept_Snapshot.txt" {}}}}}
       (test-utils/fs-tree->tree-map write-path)))

  )
