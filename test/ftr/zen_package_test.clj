(ns ftr.zen-package-test
  (:require [ftr.core]
            [ftr.pull.core]
            [clojure.test :as t]
            [zen.package]
            [matcho.core :as matcho]
            [ftr.utils.core]
            [test-utils]
            [ftr.zen-package]
            [cheshire.core]
            [zen.core]))


(def icd10-no-header-csv-content
  "10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Accidents;10344;;1;
15062;20012;W00-X59;Other external causes of accidental injury;16003;;1;10/07/2020
11748;2001203;W50-W64;Exposure to animate mechanical forces;15062;;1;
11870;2001203W64;W64;Exposure to other and unspecified animate mechanical forces;11748;;1;
11871;2001203W640;W64.0;Exposure to other and unspecified animate mechanical forces home while engaged in sports activity;11870;;1;
11872;2001203W641;W64.00;Exposure to other and unspecified animate mechanical forces, home, while engaged in sports activity;11871;;1;
11873;2001203W641;W64.01;Exposure to other and unspecified animate mechanical forces, home, while engaged in leisure activity;11871;;1;")


(defn test-zen-repos [root-path]
  {'profile-lib {:deps #{['zen-fhir (str (System/getProperty "user.dir") "/zen.fhir/")]}
                 :resources {"icd-10.csv" icd10-no-header-csv-content}
                 :zrc #{{:ns 'profile
                         :import #{'zen.fhir}

                         'diagnosis-vs
                         {:zen/tags #{'zen.fhir/value-set}
                          :zen.fhir/version "0.5.0"
                          :uri "diagnosis-vs"
                          :ftr {:module            "ftr"
                                :source-url        (str root-path "/profile-lib/resources/icd-10.csv")
                                :ftr-path          (str root-path "/profile-lib")
                                :tag               "v1"
                                :source-type       :flat-table
                                :extractor-options {:format "csv"
                                                    :csv-format {:delimiter ";"
                                                                 :quote     "'"}
                                                    :header      false
                                                    :data-row    0
                                                    :mapping     {:concept {:code    {:column 2}
                                                                            :display {:column 3}}}
                                                    :code-system {:id  "icd10"
                                                                  :url "http://hl7.org/fhir/sid/icd-10"}
                                                    :value-set   {:id   "icd10"
                                                                  :name "icd10.accidents"
                                                                  :url  "diagnosis-vs"}}}}

                         'sch
                         '{:zen/tags #{zen/schema zen.fhir/structure-schema}
                           :zen.fhir/version "0.5.0"
                           :type zen/map
                           :keys {:diagnosis {:type zen/string
                                              :zen.fhir/value-set {:symbol diagnosis-vs}}}}}}}

   'test-module {:deps '#{profile-lib}
                 :zrc '#{{:ns main
                          :import #{profile}

                          sch {:zen/tags #{zen/schema}
                               :confirms #{profile/sch}
                               :type zen/map
                               :require #{:diagnosis}}}}}})


(t/deftest init-test
  (def test-dir-path "/tmp/ftr.zen-package-test")
  (def profile-lib-path (str test-dir-path "/profile-lib"))
  (def module-dir-path (str test-dir-path "/test-module"))

  (test-utils/rm-fixtures test-dir-path)

  (test-utils/mk-fixtures test-dir-path (test-zen-repos test-dir-path))

  (t/testing "ftr extracted & shaped correctly"
    (t/testing "extracting ftr succesfully"
      (def build-ftr-ztx (zen.core/new-context {:package-paths [profile-lib-path]}))

      (zen.core/read-ns build-ftr-ztx 'profile)

      (ftr.zen-package/build-ftr build-ftr-ztx))

    (t/testing "built ftr shape is ok"
      (matcho/match
        (test-utils/fs-tree->tree-map profile-lib-path)
        {"ftr" {"tags" {"v1.ndjson.gz" {}}
                "vs"   {"diagnosis-vs"
                        {"tf.19a60d6f399157796ebc47975b7e5b882cbbb2ac8833483a1dae74c76a255a9f.ndjson.gz" {}
                         "tag.v1.ndjson.gz" {}}}}})))

  (test-utils/sh! "git" "add" "." :dir profile-lib-path)
  (test-utils/sh! "git" "commit" "-m" "Ftr initial release" :dir profile-lib-path)

  (zen.package/zen-init-deps! module-dir-path)

  (def ztx (zen.core/new-context {:package-paths [module-dir-path]}))

  (zen.core/read-ns ztx 'main)

  (t/testing "no errors in pulled package"
    (t/is (empty? (zen.core/errors ztx))))

  (t/testing "pulled ftr shape is ok"
    (matcho/match
      (test-utils/fs-tree->tree-map module-dir-path)
      {"zen-packages"
       {"profile-lib"
        {"ftr" {"tags" {"v1.ndjson.gz" {}}
                "vs"   {"diagnosis-vs"
                        {"tf.19a60d6f399157796ebc47975b7e5b882cbbb2ac8833483a1dae74c76a255a9f.ndjson.gz" {}
                         "tag.v1.ndjson.gz" {}}}}}}}))

  (t/testing "ftr memory-cache builds successfully"
    (ftr.zen-package/ftr->memory-cache ztx)

    (matcho/match
      (get @ztx :zen.fhir/ftr-cache)
      {"v1"
       {"diagnosis-vs"
        ["V01-X59"
         "W00-X59"
         "W50-W64"
         "W64"
         "W64.0"
         "W64.00"
         "W64.01"
         "XX"
         nil?]}}))

  (t/testing "vs validation works"
    (matcho/match (zen.core/validate ztx #{'main/sch} {:diagnosis "incorrect-diagnosis"})
                  {:errors [{:type ":zen.fhir/value-set"
                             :path [:diagnosis nil]}
                            nil]})

    (matcho/match (zen.core/validate ztx #{'main/sch} {:diagnosis "W64.0"})
                  {:errors empty?})))


(def gender-concepts
  [{:code "male" :display "Male"}
   {:code "female" :display "Female"}
   {:code "other" :display "Other"}
   {:code "unknown" :display "Unknown"}])


(def gender-codesystem
  {:resourceType "CodeSystem"
   :id "gender-cs-id"
   :url "gender-cs"
   :status "active"
   :content "complete"
   :valueSet "gender-vs"
   :concept gender-concepts})


(def gender-valueset
  {:resourceType "ValueSet"
   :id "gender-vs-id"
   :url "gender-vs"
   :status "active"
   :compose {:include [{:system "gender-cs"}]}})


(def ig-manifest
  {:name "ig.core"
   :version "0.0.1"
   :type "ig.core"
   :title "ig.core"
   :description "ig.core"
   :author "hs"
   :url "dehydrated"})


(defn test-zen-repos-ig [root-path]
  {'profile-lib {:deps #{['zen-fhir (str (System/getProperty "user.dir") "/zen.fhir/")]}
                 :resources {"ig/node_modules/gender-codesystem.json" (cheshire.core/generate-string gender-codesystem)
                             "ig/node_modules/gender-valueset.json" (cheshire.core/generate-string gender-valueset)
                             "ig/node_modules/package.json" (cheshire.core/generate-string ig-manifest)}
                 :zrc #{{:ns 'profile
                         :import #{'zen.fhir}

                         'gender-vs
                         {:zen/tags #{'zen.fhir/value-set}
                          :zen.fhir/version "0.5.0"
                          :uri "diagnosis-vs"
                          :ftr {:module            "ftr"
                                :source-url        (str root-path "/profile-lib/resources/ig/node_modules")
                                :ftr-path          (str root-path "/profile-lib")
                                :tag               "v1"
                                :source-type       :ig}}

                         'sch
                         '{:zen/tags #{zen/schema zen.fhir/structure-schema}
                           :zen.fhir/version "0.5.0"
                           :type zen/map
                           :keys {:diagnosis {:type zen/string
                                              :zen.fhir/value-set {:symbol gender-vs}}}}}}}

   'test-module {:deps '#{profile-lib}
                 :zrc '#{{:ns main
                          :import #{profile}

                          sch {:zen/tags #{zen/schema}
                               :confirms #{profile/sch}
                               :type zen/map
                               :require #{:gender}}}}}})


(t/deftest zen-package-with-ig-ftr
  (def test-dir-path "/tmp/ftr-ig.zen-package-test")
  (def profile-lib-path (str test-dir-path "/profile-lib"))
  (def module-dir-path (str test-dir-path "/test-module"))

  (test-utils/rm-fixtures test-dir-path)

  (test-utils/mk-fixtures test-dir-path (test-zen-repos-ig test-dir-path))

  (t/testing "ftr extracted & shaped correctly"
    (t/testing "extracting ftr succesfully"
      (def build-ftr-ztx (zen.core/new-context {:package-paths [profile-lib-path]}))

      (zen.core/read-ns build-ftr-ztx 'profile)

      (ftr.zen-package/build-ftr build-ftr-ztx))

    (t/testing "built ftr shape is ok"
      (matcho/match
        (test-utils/fs-tree->tree-map profile-lib-path)
        {"ftr" {"tags" {"v1.ndjson.gz" {}}
                "vs"   {"gender-cs-entire-code-system" nil
                        "gender-vs" {}}}}))))


(def gender2-codesystem
  {:resourceType "CodeSystem"
   :id "gender2-cs-id"
   :url "gender2-cs"
   :status "active"
   :content "complete"
   :concept gender-concepts})

(def gender2-valueset
  {:resourceType "ValueSet"
   :id "gender2-vs-id"
   :url "gender2-vs"
   :status "active"
   :compose {:include [{:system "gender2-cs"}]}})

(def gender3-valueset
  {:resourceType "ValueSet"
   :id "gender3-vs-id"
   :url "gender3-vs"
   :status "active"
   :compose {:include [{:system "gender3-cs"}]}})

(def gender4-codesystem
  {:resourceType "CodeSystem"
   :id "gender4-cs-id"
   :url "gender4-cs"
   :status "active"
   :content "complete"
   :concept gender-concepts})

(def gender5-codesystem
  {:resourceType "CodeSystem"
   :id "gender5-cs-id"
   :url "gender5-cs"
   :status "active"
   :content "complete"
   :valueSet "gender5-vs"
   :concept gender-concepts})

(def gender6-valueset
  {:resourceType "ValueSet"
   :id "gender6-vs-id"
   :url "gender6-vs"
   :status "active"
   :compose {:include [{:system "gender6-cs"
                        :concept gender-concepts}]}})

#_(def gender7-valueset #_"TODO: backport valueset expansion processing"
  {:resourceType "ValueSet"
   :id "gender7-vs-id"
   :url "gender7-vs"
   :status "active"
   :compose {:include [{:system "gender7-cs"}]}
   :expansion {:contains (mapv #(assoc % :system "gender7-cs")
                               gender-concepts)}})

(defn test-ig-ftr-zen-lib [root-path]
  {'ftr-lib {:deps #{['zen-fhir (str (System/getProperty "user.dir") "/zen.fhir/")]}
             :resources {"ig/node_modules/gender2-codesystem.json" (cheshire.core/generate-string gender2-codesystem)
                         "ig/node_modules/gender2-valueset.json" (cheshire.core/generate-string gender2-valueset)
                         "ig/node_modules/gender3-valueset.json" (cheshire.core/generate-string gender3-valueset)
                         "ig/node_modules/gender4-codesystem.json" (cheshire.core/generate-string gender4-codesystem)
                         "ig/node_modules/gender5-codesystem.json" (cheshire.core/generate-string gender5-codesystem)
                         "ig/node_modules/gender6-valueset.json" (cheshire.core/generate-string gender6-valueset)
                         #_#_"ig/node_modules/gender7-valueset.json" (cheshire.core/generate-string gender7-valueset)
                         "ig/node_modules/package.json" (cheshire.core/generate-string ig-manifest)}
             :zrc #{(merge {:ns 'ftr-lib
                            :import #{'zen.fhir}}
                           (reduce (fn [acc {:keys [url]}]
                                     (assoc acc (symbol url)
                                            {:zen/tags #{'zen.fhir/value-set}
                                             :zen.fhir/version "0.5.0"
                                             :uri url
                                             :ftr {:module            "ftr"
                                                   :source-url        (str root-path "/ftr-lib/resources/ig/node_modules")
                                                   :ftr-path          (str root-path "/ftr-lib")
                                                   :tag               "v1"
                                                   :source-type       :ig}}))
                                   {}
                                   [gender2-valueset gender3-valueset gender6-valueset]))}}})


(t/deftest ig-ftr-extraction-edge-cases
  (def test-dir-path "/tmp/ftr-ig.ig-ftr-extraction-edge-cases")
  (def profile-lib-path (str test-dir-path "/ftr-lib"))

  (test-utils/rm-fixtures test-dir-path)

  (test-utils/mk-fixtures test-dir-path (test-ig-ftr-zen-lib test-dir-path))

  (def build-ftr-ztx (zen.core/new-context {:package-paths [profile-lib-path]}))

  (zen.core/read-ns build-ftr-ztx 'ftr-lib)

  (t/testing "ftr.core/apply-cfg called only once"
    (let [og-apply-cfg ftr.core/apply-cfg
          fn-calls-count (atom 0)]
      (with-redefs [ftr.core/apply-cfg (fn [& args]
                                         (swap! fn-calls-count inc)
                                         (apply og-apply-cfg args))]
        (ftr.zen-package/build-ftr build-ftr-ztx)
        (matcho/match
          @fn-calls-count
          1))))

  (t/testing "built ftr shape is ok"
    (matcho/match
      (test-utils/fs-tree->tree-map profile-lib-path)
      {"ftr" {"tags" {"v1.ndjson.gz" {}}
              "vs"   {"gender2-cs-entire-code-system" nil
                      "gender2-vs" {}
                      "gender3-cs-entire-code-system" nil
                      "gender3-vs" nil
                      "gender4-cs-entire-code-system" {}
                      "gender4-vs" nil
                      "gender5-cs-entire-code-system" {}
                      "gender5-vs" nil
                      "gender6-cs-entire-code-system" nil
                      "gender6-vs" {}
                      #_#_"gender7-cs-entire-code-system" nil
                      #_#_"gender7-vs" {}}}})))


(defn test-concept-vs-backrefs [root-path]
  (let [cs (cheshire.core/generate-string
             {:resourceType "CodeSystem"
              :id "gender-cs-id"
              :url "gender-cs-url"
              :status "active"
              :content "complete"
              :concept [{:code "male" :display "Male"}
                        {:code "female" :display "Female"}
                        {:code "other" :display "Other"}
                        {:code "unknown" :display "Unknown"}]})
        vs-1 {:resourceType "ValueSet"
              :id "gender1-vs-id"
              :url "gender1-vs"
              :status "active"
              :compose {:include [{:system "gender-cs-url"}]}}
        vs-2 {:resourceType "ValueSet"
              :id "gender2-vs-id"
              :url "gender2-vs"
              :status "active"
              :compose {:include [{:system "gender-cs-url"}]}}]
    {'ftr-concept-lib {:deps #{['zen-fhir (str (System/getProperty "user.dir") "/zen.fhir/")]}
               :resources {"ig/node_modules/gender-codesystem.json" cs
                           "ig/node_modules/gender1-valueset.json" (cheshire.core/generate-string vs-1)
                           "ig/node_modules/gender2-valueset.json" (cheshire.core/generate-string vs-2)
                           "ig/node_modules/package.json" (cheshire.core/generate-string ig-manifest)}
               :zrc #{(merge {:ns 'ftr-concept-lib
                              :import #{'zen.fhir}}
                             (reduce (fn [acc {:keys [url]}]
                                       (assoc acc (symbol url)
                                              {:zen/tags #{'zen.fhir/value-set}
                                               :zen.fhir/version "0.5.0"
                                               :uri url
                                               :ftr {:module            "ftr"
                                                     :source-url        (str root-path "/ftr-concept-lib/resources/ig/node_modules")
                                                     :ftr-path          (str root-path "/ftr-concept-lib")
                                                     :tag               "v1"
                                                     :source-type       :ig}}))
                                     {}
                                     [vs-1 vs-2]))}}}))


(t/deftest concept-in-tf-have-only-one-backref-to-vs-and-correct-id
  (def test-dir-path "/tmp/conceptstuff")
  (def profile-lib-path (str test-dir-path "/ftr-concept-lib"))
  (test-utils/rm-fixtures test-dir-path)
  (test-utils/mk-fixtures test-dir-path (test-concept-vs-backrefs test-dir-path))
  (def build-ftr-ztx (zen.core/new-context {:package-paths [profile-lib-path]}))
  (zen.core/read-ns build-ftr-ztx 'ftr-concept-lib)

  (ftr.zen-package/build-ftr build-ftr-ztx)

  (t/testing "built ftr shape is ok"
    (matcho/match
      (test-utils/fs-tree->tree-map profile-lib-path)
      {"ftr" {"tags" {"v1.ndjson.gz" {}}
              "vs"   {"gender1-vs"
                      {"tag.v1.ndjson.gz" {}
                       "tf.ad113051910e4e7dfe7918309878b20fff71bf38ff052300daa5bc67cbf819dd.ndjson.gz" {}}
                      "gender2-vs"
                      {"tag.v1.ndjson.gz" {}
                       "tf.394a8a326de7d73aa74963b3a273e51d1960f1f3403a05dec587b04cbf5343d4.ndjson.gz" {}}}}}))

  (let [gender1-tf-path (format "%s/ftr/vs/gender1-vs/tf.ad113051910e4e7dfe7918309878b20fff71bf38ff052300daa5bc67cbf819dd.ndjson.gz"
                                profile-lib-path)
        gender2-tf-path (format "%s/ftr/vs/gender2-vs/tf.394a8a326de7d73aa74963b3a273e51d1960f1f3403a05dec587b04cbf5343d4.ndjson.gz"
                                profile-lib-path)
        gender1-vs-concepts (filter #(= (:resourceType %) "Concept") (ftr.utils.core/parse-ndjson-gz gender1-tf-path))
        gender2-vs-concepts (filter #(= (:resourceType %) "Concept") (ftr.utils.core/parse-ndjson-gz gender2-tf-path))]

    (t/testing "Each concept in terminology file that represents specific valueset have backreference to this exact valueset"
      (t/testing "valid for gender1-vs tf"
        (t/is (every? #(= ["gender1-vs"] (:valueset %)) gender1-vs-concepts)))

      (t/testing "valid for gender2-vs tf"
        (t/is (every? #(= ["gender2-vs"] (:valueset %)) gender2-vs-concepts))))

    (t/testing "Each concept id follows this pattern <valueset.url-concept.code>"
      (t/testing "valid for gender1-vs tf"
        (matcho/match gender1-vs-concepts
                      [{:id "gender1-vs-female"}
                       {:id "gender1-vs-male"}
                       {:id "gender1-vs-other"}
                       {:id "gender1-vs-unknown"}
                       nil?]))

      (t/testing "valid for gender2-vs tf"
        (matcho/match gender2-vs-concepts
                      [{:id "gender2-vs-female"}
                       {:id "gender2-vs-male"}
                       {:id "gender2-vs-other"}
                       {:id "gender2-vs-unknown"}
                       nil?]))))

  )
