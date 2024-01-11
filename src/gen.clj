(ns gen
  (:require
   [next.jdbc :as jdbc]
   [ftr.extraction.rxnorm]
   [dsql.pg :as dsql]
   [clojure.string :as str]))

(comment ;Samples
  '{Concept
    {:zen/tags #{zen/schema zen.fhir/base-schema}
     :confirms #{zen.fhir/Resource}
     :zen.fhir/type "Concept"
     :zen.fhir/version "0.5.11"
     :type zen/map
     :require #{:system :code},
     :validation-type :open
     :keys {:resourceType {:const {:value "Concept"}}
            :deprecated {:type zen/boolean
                         :zen.fhir/type "boolean"}
            :system {:type zen/string
                     :zen.fhir/type "string"}
            :code   {:type zen/string
                     :zen.fhir/type "string"}
            :display  {:type zen/string
                       :zen.fhir/type "string"} :definition {:type zen/string
                         :zen.fhir/type "string"}
            :ancestors {:type zen/map
                        :validation-type :open}
            :hierarchy {:type zen/map
                        :validation-type :open}
            :valueset {:type zen/vector
                       :every {:type zen/string
                               :zen.fhir/type "string"}}
            :property {:type zen/map
                       :validation-type :open
                       :keys
                       {:has_ingredient ; relation (for all)
                        {:type zen/vector
                         :every {:type zen/map
                                 :keys {:code {:type zen/string
                                               :zen.fhir/type "string"}
                                        :display {:type zen/string
                                                  :zen.fhir/type "string"}}}}
                        :rxn_human_drug ;attr single
                        {:type zen/string
                         :zen.fhir/type "string"}

                        :rxn_am
                        {:type zen/vector
                         :every {:type zen/string
                                               :zen.fhir/type "string"}}}}}}

    has-ingredient-code
    {:zen/tags #{aidbox.search-parameter.v1/search-parameter}
     :name "hasIngredientCode"
     :type :token
     :resource {:resourceType "Entity" :id "Concept"}
     :expression [["property" "has_ingredient" "code"]]}

    has-ingredient-display
    {:zen/tags #{aidbox.search-parameter.v1/search-parameter}
     :name "hasIngredientDisplay"
     :type :string
     :resource {:resourceType "Entity" :id "Concept"}
     :expression [["property" "has_ingredient" "display"]]}

    has-ingredient-code-index
    {:zen/tags #{aidbox.index.v1/auto-index}
     :for has-ingredient-code}

    has-ingredient-display-index
    {:zen/tags #{aidbox.index.v1/auto-index}
     :for has-ingredient-display}

    concept-repository
    {:zen/tags #{aidbox.repository.v1/repository}
     :resourceType "Concept"
     :base-profile Concept
     :indexes #{has-ingredient-code-index has-ingredient-display-index}
     :extra-parameter-sources :all
     :search-parameters #{has-ingredient-code has-ingredient-display}}
    })

(def rxnsat-atn-collections
  {:all     ["AMBIGUITY_FLAG"
             "NDC"
             "ORIG_CODE"
             "ORIG_SOURCE"
             "RXN_ACTIVATED"
             "RXN_AI"
             "RXN_AM"
             "RXN_AVAILABLE_STRENGTH"
             "RXN_BN_CARDINALITY"
             "RXN_BOSS_FROM"
             "RXN_BOSS_STRENGTH_DENOM_UNIT"
             "RXN_BOSS_STRENGTH_DENOM_VALUE"
             "RXN_BOSS_STRENGTH_NUM_UNIT"
             "RXN_BOSS_STRENGTH_NUM_VALUE"
             "RXN_HUMAN_DRUG"
             "RXN_IN_EXPRESSED_FLAG"
             "RXN_OBSOLETED"
             "RXN_QUALITATIVE_DISTINCTION"
             "RXN_QUANTITY"
             "RXN_STRENGTH"
             "RXN_VET_DRUG"
             "RXTERM_FORM"]

   :multi   ["ORIG_SOURCE"
             "RXN_AM"
             "RXN_BOSS_FROM"
             "AMBIGUITY_FLAG"
             "RXN_AVAILABLE_STRENGTH"
             "RXN_OBSOLETED"
             "RXN_ACTIVATED"
             "ORIG_CODE"
             "RXN_AI"]

   :single  ["RXN_IN_EXPRESSED_FLAG"
             "RXN_HUMAN_DRUG"
             "RXN_BOSS_STRENGTH_DENOM_VALUE"
             "RXN_BOSS_STRENGTH_NUM_VALUE"
             "RXN_QUANTITY"
             "RXN_BOSS_STRENGTH_NUM_UNIT"
             "RXN_BOSS_STRENGTH_DENOM_UNIT"
             "RXN_BN_CARDINALITY"
             "RXN_STRENGTH"
             "RXN_VET_DRUG"
             "RXTERM_FORM"
             "RXN_QUALITATIVE_DISTINCTION"]})

(comment
  (def helpful-map
    (let [connection (jdbc/get-connection "jdbc:postgresql://localhost:5125/ftr?user=ftr&password=password")
          multi-attrs (:multi rxnsat-atn-collections)
          single-attrs (:single rxnsat-atn-collections)
          rels (->>
                (dsql/format {:ql/type :pg/select
                              :select {:rel [:distinct :rela]}
                              :from    :rxnrel})
                (jdbc/execute! connection)
                (map #(get % :rxnrel/rel))
                (filter #(not (nil? %))))
          rels (->> rels
                    (reduce
                     (fn [acc value]
                       (assoc acc (keyword (str/lower-case value)) :multi-rel))
                     {}))
          multi-attrs (->> multi-attrs
                           (reduce
                            (fn [acc value]
                              (assoc acc (keyword (str/lower-case value)) :multi-attr))
                            {}))
          single-attrs (->> single-attrs
                            (reduce
                             (fn [acc value]
                               (assoc acc (keyword (str/lower-case value)) :single-attr))
                             {}))
          attrs  (merge rels multi-attrs single-attrs)]
      attrs)))

(defn to-zen-name [kw & [postfix]]
  (let [res (-> kw
                (str)
                (str/replace-first #":" "")
                (str/lower-case)
                (str/replace #"_" "-"))]
    (if postfix
      (symbol (str res "-" postfix))
      (symbol res))))

(defn to-camel-name [input-string & [postfix]]
  (let [input-string (str/replace-first (str input-string) #":" "")
        words (str/split input-string #"[\s_-]+")
        res (str/join "" (cons (str/lower-case (first words)) (map str/capitalize (rest words))))]
    (if postfix
      (str res (str/capitalize postfix))
      res)))

(defn kw-to-string [kw]
  (-> kw
      (str)
      (str/replace-first #":" "")
      ))

(defn generate-concept-schema []
  (let [all-keys (reduce
                  (fn [acc [k v]]
                    (case v
                      :single-attr
                      (assoc acc
                             (keyword (str/lower-case (kw-to-string k)))
                             '{:type zen/string
                               :zen.fhir/type "string"})
                      :multi-attr
                      (assoc acc
                             (keyword (str/lower-case (kw-to-string k)))
                             '{:type zen/vector
                               :every {:type zen/string
                                       :zen.fhir/type "string"}})
                      :multi-rel
                      (assoc acc
                             (keyword (str/lower-case (kw-to-string k)))
                             '{:type zen/vector
                               :every {:type zen/map
                                       :keys {:code {:type zen/string
                                                     :zen.fhir/type "string"}
                                              :display {:type zen/string
                                                        :zen.fhir/type "string"}}}})))
                  {}
                  helpful-map)]
    `{:zen/tags #{zen/schema zen.fhir/base-schema}
      :confirms #{zen.fhir/Resource}
      :zen.fhir/type "Concept"
      :zen.fhir/version "0.5.11"
      :type zen/map
      :require #{:system :code},
      :validation-type :open
      :keys {:resourceType {:const {:value "Concept"}}
             :deprecated {:type zen/boolean
                          :zen.fhir/type "boolean"}
             :system {:type zen/string
                      :zen.fhir/type "string"}
             :code   {:type zen/string
                      :zen.fhir/type "string"}
             :display  {:type zen/string
                        :zen.fhir/type "string"}
             :definition {:type zen/string
                          :zen.fhir/type "string"}
             :ancestors {:type zen/map
                         :validation-type :open}
             :hierarchy {:type zen/map
                         :validation-type :open}
             :valueset {:type zen/vector
                        :every {:type zen/string
                                :zen.fhir/type "string"}}
             :property {:type zen/map
                        :validation-type :open
                        :keys ~all-keys}}}
    ))


(defn generate-search-schemas []
  (let [all-keys (reduce
                  (fn [acc [k v]]
                    (case v
                      :single-attr
                      (assoc acc
                             (to-zen-name k)
                             `{:zen/tags #{aidbox.search-parameter.v1/search-parameter}
                               :name ~(to-camel-name k)
                               :type :string
                               :resource {:resourceType "Entity" :id "Concept"}
                               :expression [["property" ~(kw-to-string k)]]})
                      :multi-attr
                      (assoc acc
                             (to-zen-name k)
                             `{:zen/tags #{aidbox.search-parameter.v1/search-parameter}
                               :name ~(to-camel-name k)
                               :type :string
                               :resource {:resourceType "Entity" :id "Concept"}
                               :expression [["property" ~(kw-to-string k)]]})
                      :multi-rel
                      (assoc acc
                             (to-zen-name k "code")
                             `{:zen/tags #{aidbox.search-parameter.v1/search-parameter}
                               :name ~(to-camel-name k "code")
                               :type :token
                               :resource {:resourceType "Entity" :id "Concept"}
                               :expression [["property" ~(kw-to-string k) "code"]]}
                             (to-zen-name k "display")
                             `{:zen/tags #{aidbox.search-parameter.v1/search-parameter}
                               :name ~(to-camel-name k "display")
                               :type :string
                               :resource {:resourceType "Entity" :id "Concept"}
                               :expression [["property" ~(kw-to-string k) "display"]]})))
                  {}
                  helpful-map)]
    all-keys))

(defn generate-index-schemas []
  (let [all-keys (reduce
                  (fn [acc [k v]]
                    (case v
                      :single-attr
                      (assoc acc
                             (to-zen-name k "index")
                             `{:zen/tags #{aidbox.index.v1/auto-index}
                               :for ~(to-zen-name k)})
                      :multi-attr
                      (assoc acc
                             (to-zen-name k "index")
                             `{:zen/tags #{aidbox.index.v1/auto-index}
                               :for ~(to-zen-name k)})
                      :multi-rel
                      (assoc acc
                             (to-zen-name k "code-index")
                             `{:zen/tags #{aidbox.index.v1/auto-index}
                               :for ~(to-zen-name k "code")}
                             (to-zen-name k "display-index")
                             `{:zen/tags #{aidbox.index.v1/auto-index}
                               :for ~(to-zen-name k "display")})))
                  {}
                  helpful-map)]
    all-keys))

(comment
  (generate-concept-schema)

  (generate-search-schemas)

  (generate-index-schemas)

  (merge
   `{~(symbol nil "Concept")
     ~(generate-concept-schema)

     ~(symbol nil "concept-repository")
     {:zen/tags #{aidbox.repository.v1/repository}
      :resourceType "Concept"
      :base-profile ~(symbol nil "Concept")
      :indexes ~(-> (generate-index-schemas)
                    keys
                    set)
      :extra-parameter-sources :all
      :search-parameters ~(-> (generate-search-schemas)
                              keys
                              set)}}
   (generate-index-schemas)
   (generate-search-schemas))


  )
