(ns ftr.extraction.ig.value-set-expand-test
  (:require [ftr.extraction.ig.value-set-expand :as sut]
            [clojure.string :as str]
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
              "vs2" {"sys1" #{"code2" "code3"}}}
             (-> {}
                 (sut/push-entries-to-vs-queue
                   [{:vs-url     "vs3"
                     :system     "sys1"
                     :check-fn   (fn [concept] (= "code2" (:code concept)))
                     :depends-on ["vs2"]}
                    {:vs-url     "vs2"
                     :system     "sys1"
                     :check-fn   nil
                     :depends-on ["vs1"]}
                    {:vs-url     "vs4"
                     :system     nil
                     :check-fn   nil
                     :depends-on ["vs1"]}
                    {:vs-url     "vs4"
                     :system     nil
                     :check-fn   nil
                     :depends-on ["vs2"]}]
                   :include)
                 (sut/push-entries-to-vs-queue
                   [{:vs-url     "vs4"
                     :system     nil
                     :check-fn   (fn [concept] (str/ends-with? (:code concept) "3"))
                     :depends-on ["vs2"]}]
                   :exclude))))))
