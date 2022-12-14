(ns ftr.utils.core-test
  (:require [ftr.utils.core :as sut]
            [clojure.test :as t]
            [cheshire.core]))


(t/deftest canonical-json-map-preparation-test
  (t/testing "Prepared map is identical regardless of the initial ordering of map’s keys"
    (t/is
     (=
      (cheshire.core/generate-string
       (sut/prepare-map-for-canonical-json-generation
        {:a true
         :b true
         :c {:c1 true, :c2 true, :c3 true}
         :d [{:d1 true, :d2 true}
             {:d3 true, :d4 true}
             {:d5 true, :d6 true}]
         :e '({:e1 true, :e2 true}
              {:e3 true, :e4 true}
              {:e5 true, :e6 true})
         :f #{{:f1 true, :f2 true}
              {:f3 true, :f4 true}
              {:f5 true, :f6 true}}}))
      (cheshire.core/generate-string
       (sut/prepare-map-for-canonical-json-generation
        {:b true
         :a true
         :c {:c2 true, :c1 true, :c3 true}
         :d [{:d2 true, :d1 true}
             {:d4 true, :d3 true}
             {:d5 true, :d6 true}]
         :e '({:e2 true, :e1 true}
              {:e4 true, :e3 true}
              {:e5 true, :e6 true})
         :f #{{:f2 true, :f1 true}
              {:f4 true, :f3 true}
              {:f5 true, :f6 true}}}))
      (cheshire.core/generate-string
       (sut/prepare-map-for-canonical-json-generation
        {:c {:c1 true, :c3 true, :c2 true}
         :d [{:d2 true, :d1 true}
             {:d4 true, :d3 true}
             {:d5 true, :d6 true}]
         :b true
         :a true
         :e '({:e2 true, :e1 true}
              {:e4 true, :e3 true}
              {:e6 true, :e5 true})
         :f #{{:f2 true, :f1 true}
              {:f3 true, :f4 true}
              {:f6 true, :f5 true}}}))))))
