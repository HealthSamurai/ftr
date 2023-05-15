(ns ftr.ci-pipelines.loinc.core
  (:require [clj-http.client :as client]
            [clj-http.cookies]
            [clj-http.core]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [ftr.ci-pipelines.utils]
            [ftr.core]
            [ftr.logger.core]
            [ftr.utils.unifn.core :as u]
            [ftr.utils.core :as sut]
            [clojure.java.shell :as shell])
  (:import java.io.File))


(def
  ^{:doc "Config defaults for `pipeline`.
          What config entries you should provide for fully working pipeline:
             `gs-object-url` - url to module bucket
             `gs-ftr-object-url` - url to ftr bucket
             `loinc-login` - LOINC account login
             `loinc-password` - LOINC accout password"}
  config-defaults
  {:loinc-login-url    "https://loinc.org/wp-login.php"
   :loinc-download-url "https://loinc.org/download/loinc-complete/"

   :ftr-path             "/tmp/ftr"
   :working-dir-path     "/tmp/loinc_work_dir"
   :db                   "jdbc:postgresql://localhost:5125/ftr?user=ftr&password=password"
   :langs                []})


(defn- extract-loinc-version [loinc-service-resp]
  (let [normalized-headers    (update-keys (get loinc-service-resp :headers)
                                           str/lower-case)
        content-dispositition (get normalized-headers "content-disposition" "")]
    (second (re-find #"filename=.*?(\d+(?:\.\d+)?).*" content-dispositition))))

(defmethod u/*fn ::get-loinc-bundle!
  [{:as _ctx, :keys [loinc-login-url loinc-download-url
                     loinc-login loinc-password
                     working-dir-path]}]
  (println ::debug working-dir-path)
  (prn ::debug (seq loinc-login) (seq loinc-password))

  (let [_ (.mkdirs (io/file working-dir-path))
        download-destination (str working-dir-path \/ "loinc-bundle.zip")
        extract-destination (str working-dir-path \/ "uncompessed-loinc-bundle")

        loinc-response
        (binding [clj-http.core/*cookie-store* (clj-http.cookies/cookie-store)]
          (client/post loinc-login-url {:form-params {"log" loinc-login
                                                      "pwd" loinc-password}})

          (client/post loinc-download-url {:form-params {"tc_accepted" "1"
                                                         "tc_submit"   "Download"}
                                           :as          :byte-array}))

        response-body
        (:body loinc-response)]

    (with-open [w (io/output-stream download-destination)]
      (.write w response-body)
      (ftr.ci-pipelines.utils/unzip-file! download-destination extract-destination))

    (shell/sh "ls" "-halt")

    {:loinc-version (extract-loinc-version loinc-response)
     :extract-destination extract-destination
     :download-destination download-destination}))


(defmethod u/*fn ::build-ftr-cfg
  [{:as _ctx,
    :keys [db ftr-path loinc-version extract-destination langs
           join-original-language-as-designation]}]
  {:cfg {:module            "loinc"
         :source-url        extract-destination
         :ftr-path          ftr-path
         :tag               "prod"
         :source-type       :loinc
         :extractor-options {:db db
                             :join-original-language-as-designation join-original-language-as-designation
                             :code-system {:resourceType "CodeSystem"
                                           :id (str "loinc-" loinc-version)
                                           :url "http://loinc.org"
                                           :description "LOINC is a freely available international standard for tests, measurements, and observations"
                                           :content "not-present"
                                           :version loinc-version
                                           :name "LOINC"
                                           :publisher "Regenstrief Institute, Inc."
                                           :status "active"
                                           :caseSensitive false}

                             :value-set   {:id (str "loinc-" loinc-version)
                                           :resourceType "ValueSet"
                                           :version loinc-version
                                           :compose { :include [{:system "http://loinc.org"}]}
                                           :status "active"
                                           :name "LOINC"
                                           :url "http://loinc.org/vs"}
                             :langs langs}}})


(defmethod u/*fn ::clear-working-dir
  [{:as _ctx,
    :keys [extract-destination download-destination]}]
  (ftr.utils.core/rmrf extract-destination)
  (ftr.utils.core/rmrf download-destination)
  {})


(defmethod u/*fn ::generate-loinc-zen-package
  [{:as _ctx,
    :keys [working-dir-path loinc-version]}]
  (when working-dir-path
    (io/make-parents (str working-dir-path "/zen-package.edn"))
    (spit (str working-dir-path "/zen-package.edn")
          {:deps {'zen.fhir "https://github.com/zen-fhir/zen.fhir.git"}})
    (spit (doto (io/file (str working-dir-path "/zrc/loinc.edn"))
            (-> (.getParentFile) (.mkdirs)))
          (with-out-str (clojure.pprint/pprint {'ns 'loinc
                                                'import #{'zen.fhir}
                                                'value-set
                                                {:zen/tags #{'zen.fhir/value-set}
                                                 :zen/desc "Includes all concepts from LOINC."
                                                 :zen.fhir/version (ftr.ci-pipelines.utils/get-zen-fhir-version!)
                                                 :fhir/code-systems
                                                 #{{:fhir/url "http://loinc.org"
                                                    :zen.fhir/content :bundled}}
                                                 :uri "http://loinc.org/vs"
                                                 :version loinc-version
                                                 :ftr
                                                 {:module "loinc"
                                                  :source-url "https://storage.googleapis.com"
                                                  :ftr-path "ftr"
                                                  :source-type :cloud-storage
                                                  :tag "prod"}}})))))


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
    (u/*apply [:ftr.ci-pipelines.utils/download-previous-ftr-version!
               ::get-loinc-bundle!
               ::build-ftr-cfg
               :ftr.core/apply-cfg
               ::clear-working-dir
               :ftr.ci-pipelines.utils/upload-to-gcp-bucket
               ::generate-loinc-zen-package
               :ftr.ci-pipelines.utils/push-zen-package
               :ftr.ci-pipelines.utils/send-tg-notification]
              cfg)))


(comment
  (pipeline {})

  )
