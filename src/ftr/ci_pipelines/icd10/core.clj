(ns ftr.ci-pipelines.icd10.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [miner.ftp :as ftp]
            [zen.utils]
            [ftr.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [clojure.pprint]
            [ftr.ci-pipelines.utils]))


(defmethod u/*fn ::download-latest-icd10!
  ([{:as   _ctx,
     :keys [download-dest]}]
   (ftp/with-ftp [client "ftp://anonymous:pwd@ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM"
                  :file-type :binary]
     (let [release-dirs       (->> (ftp/client-FTPFile-directories client)
                             (remove (fn [^org.apache.commons.net.ftp.FTPFile f]
                                       (= "CM- Committee" (.getName f))))
                             (sort (fn [^org.apache.commons.net.ftp.FTPFile f1
                                        ^org.apache.commons.net.ftp.FTPFile f2]
                                     (compare (.getTimestamp f2) (.getTimestamp f1)))))
           latest-release-dir ^org.apache.commons.net.ftp.FTPFile (first release-dirs)]
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
                              (filter (fn [^java.io.File f] (str/ends-with? (.getName f) ".zip")))
                              (map (fn [^java.io.File f] (.getAbsolutePath f))))]
     (doseq [zip-path downloaded-zips]
       (zen.utils/unzip! zip-path
                         download-dest)))))


(defmethod u/*fn ::build-ftr-cfg
  [{:as   _ctx,
    :keys [ftr-path download-dest release-version]}]
  {:cfg {:module            "icd10cm"
         :source-url        download-dest
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :icd10
         :extractor-options {:code-system {:id          "icd-10-cm"
                                           :url         "http://hl7.org/fhir/sid/icd-10"
                                           :version     release-version
                                           :description "International Classification of Diseases"
                                           :content     "complete"
                                           :name        "ICD-10-CM"
                                           :valueSet    "http://hl7.org/fhir/ValueSet/icd-10"}
                             :value-set   {:id          "icd-10-cm"
                                           :url         "http://hl7.org/fhir/ValueSet/icd-10"
                                           :version     release-version
                                           :name        "ICD-10-CM-All-Codes"
                                           :status      "active"
                                           :description "This value set includes all ICD-10-CM codes."}}}})


(defmethod u/*fn ::generate-icd10-cm-zen-package
  [{:as _ctx,
    :keys [working-dir-path release-version]}]
  (when working-dir-path
    (spit (str working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str working-dir-path "/zrc/icd10cm.edn"))
                (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns 'icd10cm
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags #{'zen.fhir/value-set}
                                                 :zen/desc "This value set includes all ICD-10-CM codes."
                                                 :zen.fhir/version (ftr.ci-pipelines.utils/get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url "http://hl7.org/fhir/sid/icd-10"
                                                    :zen.fhir/content :bundled}}
                                                 :uri "http://hl7.org/fhir/ValueSet/icd-10"
                                                 :version release-version
                                                 :ftr
                                                 {:module "icd10cm"
                                                  :source-url "https://storage.googleapis.com"
                                                  :ftr-path "ftr"
                                                  :source-type :cloud-storage
                                                  :tag "prod"}}})))))


(def config-defaults
  {:download-dest
   "/tmp/ftr/ci_pipelines/icd10"

   :ftr-path
   "/tmp/ftr"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))]
    (u/*apply [:ftr.ci-pipelines.utils/download-previous-ftr-version!
               ::download-latest-icd10!
               ::unpack-downloaded-icd10!
               ::build-ftr-cfg
               :ftr.core/apply-cfg
               :ftr.ci-pipelines.utils/upload-to-gcp-bucket
               ::generate-icd10-cm-zen-package
               :ftr.ci-pipelines.utils/push-zen-package
               :ftr.ci-pipelines.utils/send-tg-notification]
              cfg)))
