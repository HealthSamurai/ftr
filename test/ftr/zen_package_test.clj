(ns ftr.zen-package-test
  (:require [ftr.core]
            [ftr.pull.core]
            [clojure.test :as t]
            [zen.package]
            [matcho.core :as matcho]
            [ftr.utils.core]
            [test-utils]
            [ftr.zen-package]
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
                                              :zen.fhir.value-set {:symbol diagnosis-vs}}}}}}}

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
                  {:errors [{:type ":zen.fhir.value-set"
                             :path [:diagnosis nil]}
                            nil]})

    (matcho/match (zen.core/validate ztx #{'main/sch} {:diagnosis "W64.0"})
                  {:errors empty?})))
