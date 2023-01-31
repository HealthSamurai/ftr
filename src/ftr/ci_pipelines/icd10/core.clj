(ns ftr.ci-pipelines.icd10.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [miner.ftp :as ftp]
            [zen.utils]
            [ftr.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [clojure.java.shell :as shell]
            [clojure.pprint]))


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


(defmethod u/*fn ::download-previous-icd-10-ftr-version!
  [{:as _ctx, :keys [gs-object-url ftr-path]}]
  (when gs-object-url
    (let [{:as _execution-result
           :keys [exit err]}
          (shell/sh "gsutil" "cp" "-r" gs-object-url ftr-path)]
      (when-not (= exit 0)
        {::u/status :error
         ::u/message err}))))


(defmethod u/*fn ::upload-to-gcp-bucket
  [{:as _ctx,
    :keys [gs-ftr-object-url],
    {:keys [ftr-path module]} :cfg}]
  (when gs-ftr-object-url
    (let [{:as _execution-result
           :keys [exit err]}
          (shell/sh "gsutil" "rsync" "-r"
                    (str ftr-path \/ module)
                    (str gs-ftr-object-url \/ module))]
      (when-not (= exit 0)
        {::u/status :error
         ::u/message err}))))


(defn get-zen-fhir-version! []
  (str/trim (:out (shell/sh "git" "describe" "--tags" "--abbrev=0" :dir "zen.fhir"))))


(defmethod u/*fn ::generate-icd-10-cm-zen-package
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
                                                 :zen.fhir/version (get-zen-fhir-version!)
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


(defmethod u/*fn ::push-zen-package
  [{:as _ctx,
    :keys [working-dir-path]}]
  (when working-dir-path
    (loop [commands [["git" "add" "--all" :dir working-dir-path]
                     ["git" "commit" "-m" "Update zen package" :dir working-dir-path]
                     ["git" "push" "-u" "origin" "main" :dir working-dir-path]]]
      (when-not (nil? commands)
        (let [{:as _executed-command,
               :keys [exit err]}
              (apply shell/sh (first commands))]
          (if (or (= exit 0)
                  (not (seq err)))
            (recur (next commands))
            {::u/status :error
             ::u/message err}))))))


(def config-defaults
  {:download-dest
   "/tmp/ftr/ci_pipelines/icd10"

   :ftr-path
   "/tmp/ftr"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]))]
    (doto (u/*apply [::download-previous-icd-10-ftr-version!
                     ::download-latest-icd10!
                     ::unpack-downloaded-icd10!
                     ::build-ftr-cfg
                     :ftr.core/apply-cfg
                     ::upload-to-gcp-bucket
                     ::generate-icd-10-cm-zen-package
                     ::push-zen-package]
                    cfg))))
