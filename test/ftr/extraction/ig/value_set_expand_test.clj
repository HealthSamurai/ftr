(ns ftr.extraction.ig.value-set-expand-test
  (:require [ftr.extraction.ig.value-set-expand :as sut]
            [clojure.test :as t]))


(t/deftest nested-vs-refs-process-test
  (t/is (= {"sys0" {"code0" {:code     "code0"
                             :system   "sys0"
                             :valueset #{"vs1" "vs4"}}}
            "sys1" {"code1" {:code     "code1"
                             :system   "sys1"
                             :valueset #{"vs1" "vs2" "vs4"}}
                    "code2" {:code     "code2"
                             :system   "sys1"
                             :valueset #{"vs2" "vs4" "vs3"}}
                    "code3" {:code     "code3"
                             :system   "sys1"
                             :valueset #{"vs2"}}}}
           (sut/process-nested-vss-refs
             {"sys0" {"code0" {:code     "code0"
                               :system   "sys0"
                               :valueset #{"vs1"}}}
              "sys1" {"code1" {:code     "code1"
                               :system   "sys1"
                               :valueset #{"vs1"}}
                      "code2" {:code     "code2"
                               :system   "sys1"
                               :valueset #{"vs2"}}
                      "code3" {:code     "code3"
                               :system   "sys1"
                               :valueset #{"vs2"}}}}
             {"vs1" {"sys0" #{"code0"}
                     "sys1" #{"code1"}}
              "vs2" {"sys1" #{"code2"}}}
             {"vs3" {"sys1" {"vs2" [(fn [concept] (= "code2" (:code concept)))]}}
              "vs2" {"sys1" {"vs1" [(constantly true)]}}
              "vs4" {nil {"vs1" [(constantly true)]
                          "vs2" [(constantly true)
                                 (complement (fn [concept] (= "code3" (:code concept))))]}}}))))
