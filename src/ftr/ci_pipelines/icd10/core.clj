(ns ftr.ci-pipelines.icd10.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [miner.ftp :as ftp]
            [zen.utils]
            [ftr.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]))


(defmethod u/*fn ::download-latest-icd10!
  ([{:as   _ctx,
     :keys [download-dest]}]
   (ftp/with-ftp [client "ftp://anonymous:pwd@ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM"
                  :file-type :binary]
     (let [release-dirs       (->> (ftp/client-FTPFile-directories client)
                             (remove #(= "CM- Committee" (.getName %)))
                             (sort #(compare (.getTimestamp %2) (.getTimestamp %1))))
           latest-release-dir (first release-dirs)]
       (ftp/client-cd client (.getName latest-release-dir))

       (.mkdirs (io/file download-dest))

       (doseq [filename (ftp/client-file-names client)]
         (ftp/client-get client
                         filename
                         (format "%s/%s" download-dest filename)))
       {:release-version (.getName latest-release-dir)}))))


(defmethod u/*fn ::unpack-downloaded-icd10!
  ([{:as   _ctx,
     :keys [download-dest]}]
   (let [download-dir    (io/file download-dest)
         downloaded-zips (->> download-dir
                              (.listFiles)
                              (filter #(str/ends-with? (.getName %) ".zip"))
                              (map #(.getAbsolutePath %)))]
     (doseq [zip-path downloaded-zips]
       (zen.utils/unzip! zip-path
                         download-dest)))))


(defmethod u/*fn ::build-ftr-cfg
  [{:as   _ctx,
    :keys [ftr-path download-dest release-version]}]
  {:cfg {:module            "icd10"
         :source-url        download-dest
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :icd10
         :extractor-options {:code-system {:id          "icd-10"
                                           :url         "http://hl7.org/fhir/sid/icd-10"
                                           :version     release-version
                                           :description "International Classification of Diseases"
                                           :content     "complete"
                                           :name        "ICD-10-CM"
                                           :valueSet    "http://hl7.org/fhir/ValueSet/icd-10"}
                             :value-set   {:id          "icd-10"
                                           :url         "http://hl7.org/fhir/ValueSet/icd-10"
                                           :version     release-version
                                           :name        "ICD-10-CM-All-Codes"
                                           :status      "active"
                                           :description "This value set includes all ICD-10 codes."}}}})


(def config-defaults
  {:download-dest
   "/tmp/ftr/ci_pipelines/icd10"

   :ftr-path
   "/tmp/ftr"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))]
    (doto (u/*apply [::download-latest-icd10!
                     ::unpack-downloaded-icd10!
                     ::build-ftr-cfg
                     :ftr.core/apply-cfg]
                    cfg))))
