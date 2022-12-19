(ns ftr.extraction.ig.value-set-expand-test
  (:require [ftr.extraction.ig.value-set-expand :as sut]
            [clojure.string :as str]
            [matcho.core :as match]
            [clojure.test :as t]))


#_"NOTE: Concept resource"
{:code "string"
 :system "string"}

#_"NOTE: ValueSet resource"
{:url "string"
 :compose {:include [#_"NOTE: contains union of rules"
                     {#_"NOTE: rule defining a selection of concepts"
                      #_"NOTE: Must have :system OR :valueSet. Can have both"
                      #_"NOTE: Can't have both :concept and :filter. Can have neither"
                      :system "string"
                      :concept [{:code "string"} #_...]}
                     {:system "string"
                      :filter {:property "string"
                               :op "string" #_"NOTE: = | is-a | descendent-of | is-not-a | regex | in | not-in | generalizes | exists"
                               :value "string"}}
                     {:valueSet ["string" #_"NOTE: intersection of other value sets"]}]
           :exclude [#_"NOTE: structure is the same as include"
                     #_"NOTE: excludes union of rules here from the union defined in include"
                     {}]}}

#_"NOTE: CodeSystem index"
{"<Concept.system>"
 {"<Concept.code>"
  {#_"<Concept resource>"
   :valueset ["<ValueSet.url>"]
   #_"NOTE: this attr is empty before the start of the algorithm, must be populated as the result"}}}

#_"NOTE: ValueSet.compose process queue"
{"<ValueSet.url>"
 {:deps #{"<ValueSet.url>"}

  :include {:systems
            {"<Concept.system>" {["<ValueSet.url>"] #_"NOTE: value set intersection, can be empty"
                                 {#_"NOTE: can't have both"
                                  :allow-any-concept true
                                  :pred-fn [#_"function that accepts a Concept and returns a boolean"]}}}

            :any-system #_"NOTE: when no system specified, deduce from provided value sets"
            {["<ValueSet.url>"] #_"NOTE: value set intersection, CAN NOT be empty"
             {#_"NOTE: can't have both"
              :allow-any-concept true
              :pred-fn [#_"function that accepts a Concept and returns a boolean"]}}}
  :exclude {#_"same structure as :include"}

  :full-expansion? true ;; boolean
  :expansion-index {"<Concept.system>"
                    #{"<Concept.code>"}}}}

#_"NOTE: ValueSet content index"
{"<ValueSet.url>"
 {"<Concept.system>"
  #{"<Concept.code>"}}}

#_"NOTE: after this index is produced it is normalized into the CodeSystem index [system code :valueset]"


(t/deftest nested-vs-refs-process-test

  (def concepts-index-fixture
    {"sys1" {"code11" {:system "sys1"
                       :code "code11"}
             "code12" {:system "sys1"
                       :code "code12"}}
     "sys2" {"code21" {:system "sys2"
                       :code "code21"}
             "code22" {:system "sys2"
                       :code "code22"}}})

  (def concepts-index-fixture
    (-> concepts-index-fixture
        (update-vals (fn [vls]
                       (update-vals vls
                                    #(-> %
                                         (assoc :zen.fhir/system-kw (keyword (:system %)))
                                         (assoc :zen.fhir/code-kw (keyword (:code %)))))))))

  (def valuesets-fixtures
    [{:url "simple-include"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]}}
     {:url "simpler-include"
      :compose {:include [{:system "sys2"}]}}
     {:url "include-with-concept-enumeration"
      :compose {:include [{:system "sys1"
                           :concept [{:code "code11"}]}
                          {:system "sys2"}]}}
     {:url "include-with-filter"
      :compose {:include [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "code11"}]}
                          {:system "sys2"}]}}

     {:url "empty-vs"
      :compose {:include [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "???"}]}]}}

     {:url "full-expansion"
      :compose {:include [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "missing"
                                     :value    "foo"}]}]}
      :expansion {:total 1
                  :offset 0
                  #_#_:parameter {:offset 0 :count 1000} #_"TODO: check if there no meaningful parameters"
                  :contains [{:system "sys1" #_"TODO: add recursive :contains"
                              :code   "code12"}]}}

     {:url "not-full-expansion"
      :compose {:include [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "code11"}]}
                          {:system "sys2"
                           :filter [{:op       "is-a"
                                     :property "missing"
                                     :value    "foo"}]}]}
      :expansion {:contains [{:system "sys2"
                              :code   "code21"}]}}

     {:url "depends-on-valueset"
      :compose {:include [{:valueSet ["simple-include"]}]}}

     {:url "depends-on-valueset-intersection"
      :compose {:include [{:valueSet ["simple-include"
                                      "full-expansion"]}]}}

     {:url "intersection-with-empty-valueset"
      :compose {:include [{:valueSet ["simple-include"
                                      "empty-vs"]}]}}

     {:url "empty-intersection"
      :compose {:include [{:valueSet ["full-expansion"
                                      "depends-on-valueset-and-filters-by-sys-and-pred"]}]}}

     {:url "depends-on-valueset-intersection-and-filters-by-sys"
      :compose {:include [{:system "sys1"
                           :valueSet ["simple-include"
                                      "not-full-expansion"]}]}}

     {:url "depends-on-valueset-and-filters-by-sys"
      :compose {:include [{:system "sys1"
                           :valueSet ["simple-include"]}]}}

     {:url "depends-on-valueset-and-filters-by-sys-and-pred"
      :compose {:include [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "code11"}]
                           :valueSet ["simple-include"]}]}}

     {:url "exclude-system"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys2"}]}}
     {:url "exclude-code"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys2"
                           :concept [{:code "code22"}]}]}}
     {:url "multiple-exclude"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"
                           :concept [{:code "code12"}]}
                          {:system "sys2"
                           :concept [{:code "code22"}]}]}}
     {:url "multiple-exclude-with-vs"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"
                           :valueSet ["full-expansion"]}
                          {:system "sys2"
                           :concept [{:code "code22"}]}]}}
     {:url "multiple-exclude-with-vs-no-system"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:valueSet ["full-expansion"]}
                          {:system "sys2"
                           :concept [{:code "code22"}]}]}}
     {:url "exclude-filter"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys2"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "code22"}]}]}}
     {:url "exclude-valueset-with-system"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"
                           :valueSet ["simple-include"]}]}}
     {:url "exclude-valueset-with-pred"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"
                           :concept [{:code "code12"}]
                           :valueSet ["simple-include"]}]}}
     {:url "exclude-valueset-with-pred-not-in-vs"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "???"}]
                           :valueSet ["simple-include"]}]}}
     {:url "exclude-valueset-no-system"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:valueSet ["depends-on-valueset-intersection"]}]}}
     {:url "exclude-all"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys1"}
                          {:system "sys2"}]}}
     {:url "exclude-all-vs"
      :compose {:include [{:valueSet ["simple-include"]}]
                :exclude [{:valueSet ["simple-include"]}]}}
     {:url "exclude-valueset-system-not-present-in-vs"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:system "sys2"
                           :valueSet ["depends-on-valueset-intersection"]}]}}
     {:url "exclude-empty-valueset"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:valueSet ["empty-vs"]}]}}
     {:url "exclude-valueset-intersection"
      :compose {:include [{:system "sys1"}
                          {:system "sys2"}]
                :exclude [{:valueSet ["simple-include"
                                      "full-expansion"]}]}}
     {:url "exclude-empty-valueset-intersection"
      :compose {:include [{:system "sys1"}]
                :exclude [{:system "sys1"
                           :valueSet ["depends-on-valueset-intersection"
                                      "depends-on-valueset-intersection-and-filters-by-sys"]}]}}
     {:url "exclude-empty-valueset-intersection"
      :compose {:include [{:valueSet ["depends-on-valueset-intersection"]}]
                :exclude [{:system "sys1"
                           :valueSet ["depends-on-valueset-intersection-and-filters-by-sys"]}]}}
     {:url "exclude-empty-valueset-intersection-system"
      :compose {:include [{:system "sys1"
                           :valueSet ["depends-on-valueset-intersection"]}]
                :exclude [{:system "sys1"
                           :valueSet ["depends-on-valueset-intersection-and-filters-by-sys"]}]}}
     {:url "exclude-valueset-not-intersection-no-system"
      :compose {:include [{:valueSet ["depends-on-valueset-intersection"]}]
                :exclude [{:valueSet ["depends-on-valueset-intersection-and-filters-by-sys"]}]}}
     {:url "exclude-valueset-not-intersection-no-system-no-system-in-exclude"
      :compose {:include [{:valueSet ["simple-include"]}]
                :exclude [{:system "sys1"
                           :valueSet ["simpler-include"]}
                          {:system "sys1"
                           :filter [{:op       "is-a"
                                     :property "code"
                                     :value    "code12"}]}]}}
     {:url "exclude-with-expansion"
      :compose {:include [{:valueSet ["simple-include"]
                           :filter [{:op "is-a"
                                     :property "missing"
                                     :value  "???"}]}]
                :exclude [{:system "sys2"
                           :concept [{:code "code22"}]}]}
      :expansion {:contains [{:system "sys2"
                              :code   "code21"}]}}
     #_{:url "exclude-withexpansion"
        :compose {:include [{:system "sys1"}
                            {:system "sys2"}]
                  :exclude [{:system "sys2" #_"FIXME: WHAT TO DO WHEN WE CAN'T EVAL EXCLUDE????"
                             :filter [{:op       "is-a"
                                       :property "missing"
                                       :value    "???"}]}]}
        :expansion {:contains [{:system "sys2"
                                :code   "code21"}]}}
     {:url "include-vs-exclude-other-vs"
      :compose {:include [{:valueSet ["simpler-include" "simple-include"]}]
                :exclude [{:valueSet ["not-full-expansion"]}]}}])

  (def valuesets-index-assert
    {"simple-include" {"sys1" #{"code11" "code12"}
                       "sys2" #{"code21" "code22"}}

     "simpler-include" {"sys2" #{"code21" "code22"}}

     "include-with-concept-enumeration" {"sys1" #{"code11"}
                                         "sys2" #{"code21" "code22"}}

     "include-with-filter" {"sys1" #{"code11"}
                            "sys2" #{"code21" "code22"}}

     "empty-vs" {}

     "full-expansion" {"sys1" #{"code12"}}

     "not-full-expansion" {"sys1" #{"code11"}
                           "sys2" #{"code21"}}

     "depends-on-valueset" {"sys1" #{"code11" "code12"}
                            "sys2" #{"code21" "code22"}}

     "depends-on-valueset-intersection" {"sys1" #{"code12"}}

     "depends-on-valueset-intersection-and-filters-by-sys" {"sys1" #{"code11"}}

     "intersection-with-empty-valueset" {}

     "empty-intersection" {}

     "depends-on-valueset-and-filters-by-sys" {"sys1" #{"code11" "code12"}}

     "depends-on-valueset-and-filters-by-sys-and-pred" {"sys1" #{"code11"}}

     "exclude-system" {"sys1" #{"code11" "code12"}}
     "exclude-code" {"sys1" #{"code11" "code12"}
                     "sys2" #{"code21"}}
     "multiple-exclude" {"sys1" #{"code11"}
                         "sys2" #{"code21"}}
     "multiple-exclude-with-vs" {"sys1" #{"code11"}
                                 "sys2" #{"code21"}}
     "multiple-exclude-with-vs-no-system" {"sys1" #{"code11"}
                                           "sys2" #{"code21"}}
     "exclude-filter" {"sys1" #{"code11" "code12"}
                       "sys2" #{"code21"}}
     "exclude-valueset-with-system" {"sys2" #{"code21" "code22"}}
     "exclude-valueset-with-pred" {"sys1" #{"code11"}
                                   "sys2" #{"code21" "code22"}}
     "exclude-valueset-with-pred-not-in-vs" {"sys1" #{"code11" "code12"}
                                             "sys2" #{"code21" "code22"}}
     "exclude-valueset-no-system" {"sys1" #{"code11"}
                                   "sys2" #{"code21" "code22"}}
     "exclude-all" {}
     "exclude-all-vs" {}
     "exclude-valueset-system-not-present-in-vs" {"sys1" #{"code11" "code12"}
                                                  "sys2" #{"code21" "code22"}}
     "exclude-empty-valueset" {"sys1" #{"code11" "code12"}
                               "sys2" #{"code21" "code22"}}
     "exclude-valueset-intersection" {"sys1" #{"code11"}
                                      "sys2" #{"code21" "code22"}}
     "exclude-empty-valueset-intersection" {"sys1" #{"code12"}}
     "exclude-empty-valueset-intersection-system" {"sys1" #{"code12"}}
     "exclude-valueset-not-intersection-no-system" {"sys1" #{"code12"}}
     "exclude-valueset-not-intersection-no-system-no-system-in-exclude" {"sys1" #{"code11"}
                                                                         "sys2" #{"code21" "code22"}}
     "exclude-with-expansion" {"sys2" #{"code21"}}
     "include-vs-exclude-other-vs" {"sys2" #{"code22"}}})

  (t/is (= valuesets-index-assert
           (sut/all-vs-nested-refs->vs-idx
             concepts-index-fixture
             (sut/build-valuesets-compose-idx valuesets-fixtures)))))
