(ns ftr.ci-pipelines.snomed.core-test
  (:require [ftr.ci-pipelines.snomed.core :as sut]
            [clojure.test :as t]
            [matcho.core :as matcho]
            [clojure.string :as str]))

(t/deftest get-latest-snomed-download-url-test
  (matcho/match
    (sut/get-latest-snomed-download-url)
    #(str/starts-with? % "https://uts-ws.nlm.nih.gov/download?url=https://download.nlm.nih.gov/mlb/utsauth/USExt/SnomedCT_USEditionRF2_PRODUCTION_20220901T120000Z.zip&apiKey=")))
