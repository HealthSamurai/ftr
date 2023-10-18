(ns ftr.ci-pipelines.rxnorm.core
  (:require
   [clj-http.client :as client]
   [clojure.java.io :as io]
   [clojure.pprint]
   [ftr.ci-pipelines.utils]
   [ftr.utils.unifn.core :as u]
   [clojure.string :as str]
   [ftr.utils.core])
  (:import [java.io File]))


(def
  ^{:doc "A set of default configuration values for the RxNorm FTR pipeline,
          designed to be particularly useful in dev environments."}
  config-defaults
  {:automated-downloads-url "https://uts-ws.nlm.nih.gov/download"
   :rxnorm-current-version-url "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"
   :db "jdbc:postgresql://localhost:5125/ftr?user=ftr&password=password"
   :ftr-path "/tmp/ftr/"
   :working-dir-path "/tmp/rxnorm_work_dir"})


(defn extract-rxnorm-version
  "Searches for a 'Readme' file in the provided RxNorm bundle directory. The function assumes the 'Readme'
  file follows the naming pattern: 'Readme_full_<version>.txt'. Extracts and returns the version
  sub-string. Throws an exception if the expected pattern is not found.

  Parameters:
  - `extract-destination`: A File object pointing to the RxNorm directory.

  Returns:
  - A string representing the extracted RxNorm version."
  [^File extract-destination]
  (let [readme-file-name (->> extract-destination
                              (.listFiles)
                              (mapv (memfn ^File getName))
                              (filter #(str/starts-with? % "Readme"))
                              (first))
        rxnorm-version (-> readme-file-name
                           (str/split #"_")
                           (last)
                           (str/split #"\.")
                           (first))]
    rxnorm-version))


(defmethod u/*fn ::get-rxnorm-bundle!
  [{:as   _ctx,
    :keys [automated-downloads-url rxnorm-current-version-url
           api-key working-dir-path]}]
  (let [_                    (.mkdirs (io/file working-dir-path))
        download-destination (io/file working-dir-path "rxnorm-bundle.zip")
        extract-destination  (io/file working-dir-path "uncompessed-rxnorm-bundle")
        response             (client/get automated-downloads-url
                                         {:query-params {"url"    rxnorm-current-version-url
                                                         "apiKey" api-key}
                                          :as           :byte-array})
        response-body        (:body response)]
    (with-open [w (io/output-stream download-destination)]
      (.write w response-body)
      (ftr.ci-pipelines.utils/unzip-file! download-destination extract-destination))

    {:rxnorm-version (extract-rxnorm-version extract-destination)
     :extract-destination  extract-destination
     :download-destination download-destination}))


(defmethod u/*fn ::build-ftr-cfg
  [{:as _ctx,
    :keys [db ftr-path rxnorm-version extract-destination module]}]
  {:cfg {:module            (or module "rxnorm")
         :source-url        extract-destination
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :rxnorm
         :extractor-options {:db db
                             :code-system {:resourceType "CodeSystem"
                                           :id "rxnorm-cs"
                                           :url "http://www.nlm.nih.gov/research/umls/rxnorm"
                                           :description "RxNorm provides normalized names for clinical drugs and links its names to many of the drug vocabularies commonly used in pharmacy management and drug interaction software, including those of First Databank, Micromedex, and Gold Standard Drug Database. By providing links between these vocabularies, RxNorm can mediate messages between systems not using the same software and vocabulary. RxNorm now includes the United States Pharmacopeia (USP) Compendial Nomenclature from the United States Pharmacopeial Convention. USP is a cumulative data set of all Active Pharmaceutical Ingredients (API)."
                                           :content "not-present"
                                           :version rxnorm-version
                                           :name "RxNorm"
                                           :publisher "National Library of Medicine (NLM)"
                                           :status "active"}
                             :value-set   {:id "rxnorm-vs"
                                           :resourceType "ValueSet"
                                           :version rxnorm-version
                                           :compose { :include [{:system "http://www.nlm.nih.gov/research/umls/rxnorm"}]}
                                           :status "active"
                                           :name "RxNorm"
                                           :url "http://www.nlm.nih.gov/research/umls/rxnorm/valueset"}}}})


(defmethod u/*fn ::clear-working-dir
  [{:as _ctx,
    :keys [extract-destination download-destination]}]
  (ftr.utils.core/rmrf extract-destination)
  (ftr.utils.core/rmrf download-destination)
  {})


(defmethod u/*fn ::generate-rxnorm-zen-package
  [{:as _ctx,
    :keys [working-dir-path rxnorm-version module]}]
  (when working-dir-path
    (io/make-parents (str working-dir-path "/zen-package.edn"))
    (spit (str working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str working-dir-path "/zrc/rxnorm.edn"))
            (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns 'rxnorm
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags #{'zen.fhir/value-set}
                                                 :zen/desc "Includes all concepts from RxNorm."
                                                 :zen.fhir/version (ftr.ci-pipelines.utils/get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url "http://www.nlm.nih.gov/research/umls/rxnorm"
                                                    :zen.fhir/content :bundled}}
                                                 :uri "http://www.nlm.nih.gov/research/umls/rxnorm/valueset"
                                                 :version rxnorm-version
                                                 :ftr
                                                 {:module (or module "rxnorm")
                                                  :source-url "https://storage.googleapis.com"
                                                  :ftr-path "ftr"
                                                  :source-type :cloud-storage
                                                  :tag "prod"}}})))))


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
    (clojure.pprint/pprint
      (u/*apply [:ftr.ci-pipelines.utils/download-previous-ftr-version!
                 ::get-rxnorm-bundle!
                 ::build-ftr-cfg
                 :ftr.core/apply-cfg
                 ::clear-working-dir
                 :ftr.ci-pipelines.utils/upload-to-gcp-bucket
                 ::generate-rxnorm-zen-package
                 :ftr.ci-pipelines.utils/push-zen-package
                 :ftr.ci-pipelines.utils/send-tg-notification]
                cfg))))


(comment
  (pipeline {})

  )
