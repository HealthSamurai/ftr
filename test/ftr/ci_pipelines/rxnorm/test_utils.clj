(ns ftr.ci-pipelines.rxnorm.test-utils
  (:require
   [ftr.test-utils :as test-utils]
   [hiccup.page]
   [clojure.java.io :as io]))


(def rxnconso-content
  "3|ENG||||||8717795||58488005||SNOMEDCT_US|PT|58488005|1,4-alpha-Glucan branching enzyme||N||
3|ENG||||||8717796||58488005||SNOMEDCT_US|FN|58488005|1,4-alpha-Glucan branching enzyme (substance)||N||
3|ENG||||||8717808||58488005||SNOMEDCT_US|SY|58488005|Amylo-(1,4,6)-transglycosylase||N||
3|ENG||||||8718164||58488005||SNOMEDCT_US|SY|58488005|Branching enzyme||N||
2665428|ENG||||||12853445|12853445|2665428||RXNORM|SCD|2665428|cholecalciferol 0.075 MG / folic acid 1 MG Oral Capsule||N|4096|
2665428|ENG||||||12853448|12853448|2665428||RXNORM|SY|2665428|vitamin D 3 75 MCG (3000 UNT) / folic acid 1 MG Oral Capsule||N|4096|
2665428|ENG||||||12853449|12853449|2665428||RXNORM|PSN|2665428|vitamin D 3 75 MCG (3000 UNT) / folic acid 1 MG Oral Capsule||N|4096|
2665428|ENG||||||12853492|12853492|2665428||RXNORM|SY|2665428|cholecalciferol 0.075 MG / folate 1 MG Oral Capsule||N|4096|
2665426|ENG||||||12853438|12853438|2665426||RXNORM|SY|2665426|0.8 ML Hyrimoz 50 MG/ML Prefilled Syringe||O|4096|
2665426|ENG||||||12853441|12853441|2665426||RXNORM|SY|2665426|Hyrimoz 40 MG per 0.8 ML Prefilled Syringe||O|4096|
2665426|ENG||||||12853437|12853437|2665426||RXNORM|SCD|2665426|0.8 ML adalimumab-adaz 50 MG/ML Prefilled Syringe [Hyrimoz]||O|4096|
2665426|ENG||||||12853442|12853442|2665426||RXNORM|PSN|2665426|Hyrimoz 40 MG in 0.8 ML Prefilled Syringe||O|4096|
1337|ENG||||||12853437|12853437|2665426||RXNORM|SCD|2665426|tablets||O|4096|")

(def bundle-entries
  [["rrf/RXNCONSO.RRF" rxnconso-content]
   ["Readme_Full_10022023.txt" ""]])
