(ns ftr.ci-pipelines.icd10.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [miner.ftp :as ftp]
            [zen.utils]
            [ftr.core]
            [ftr.utils.unifn.core :as u]
            [ftr.logger.core]
            [clojure.pprint]
            [ftr.ci-pipelines.utils]
            [org.httpkit.client :as http]
            [hickory.core]
            [hickory.select :as s]
            [clojure.zip :as zip]))


(defn get-html-content-from-url! [url]
  (-> url
      (http/get)
      (deref)
      :body))


(defn parse-meta
  "Parses a string containing metadata information in a specific format and returns a map with parsed values.

  The input string should be in the following format:
  'month/day/year   time   <dir>'

  Parameters:
  - s (String): The input string containing metadata.

  Returns:
  - A map with the following keys:
    - :date (String): The date in 'YYYY-MM-DD' format.
    - :time (String): The time in 'hh:mm AM/PM' format.
    - :is-dir? (Boolean): Indicates whether the entry represents a directory (true) or not (false).

  Example:
  (parse-meta \"4/25/2019  8:15 PM        <dir>\")
  ; Returns {:date \"2019-04-25\", :time \"8:15 PM\", :is-dir? true}"
  [s]
  (let [pattern #"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}:\d{2}\s+[APMapm]{2})\s+(<dir>)?\s*$"
        matches (re-matches pattern s)]
    (when matches
      (let [month (nth matches 1)
            day   (nth matches 2)
            year  (nth matches 3)]
        {:date       (str year "-" (format "%02d" (Integer. month)) "-" (format "%02d" (Integer. day)))
         :time       (nth matches 4)
         :is-dir?    (boolean (nth matches 5))}))))


(defn get-html-doc-represented-as-hickory
  "Fetches an HTML document from a given URL, parses it, and represents it as a Hickory DOM tree.

  Parameters:
  - url (String): The URL of the HTML document to fetch and parse.

  Returns:
  - A parsed HTML document represented as a Hickory DOM tree.

  Example:
  (get-html-doc-represented-as-hickory \"https://example.com/\")
  ; Returns a Hickory DOM tree representing the HTML document from the URL."
  [url]
  (-> url
      (get-html-content-from-url!)
      (hickory.core/parse)
      (hickory.core/as-hickory)))


(defn get-adjacent-text-nodes-and-a-tags
  "
  Searches for adjacent <a> tags and text-nodes
  Parameters:
  - hickory-dom (Hickory DOM tree): The Hickory DOM tree to search for and rename <a> tags.

  Returns:
  - List of <a> tags and text-nodes
  "
  [hickory-dom]
  (s/select (s/child (s/tag :pre)
                     (s/or
                       (every-pred
                         (comp string? clojure.zip/node)
                         (fn [n]
                           (some-> (clojure.zip/right n)
                                   ((s/tag :a)))))
                       (every-pred
                         (s/tag :a)
                         (fn [n]
                           (some-> (clojure.zip/left n)
                                   (clojure.zip/node)
                                   (string?))))))
            hickory-dom))


(defn create-file-objects
  "
  Creates a list of file objects from a sequence of <a> tags adjacent to text nodes.

  Given a sequence of <a> tags, each of which is adjacent to a text node, this function
  transforms them into a list of file objects. Each file object contains metadata, an href,
  and a file name, and the list of file objects is sorted by date.

  Parameters:
  - a-tags-adjacent-with-text-nodes (Sequence): A sequence of <a> tags adjacent to text nodes.

  Returns:
  - A sorted list of file objects, where each file object is a map with the following keys:
    - :meta (Map): Metadata containing date, time, and is-dir? information.
    - :href (String): The href attribute of the <a> tag.
    - :file-name (String): The file name extracted from the <a> tag.
  "
  [a-tags-adjacent-with-text-nodes]
  (->> (partition 2 a-tags-adjacent-with-text-nodes)
       (mapv (fn [[meta a-tag]]
               {:meta (parse-meta meta)
                :href (get-in a-tag [:attrs :href])
                :file-name (get-in a-tag [:content 0])}))
       (sort-by (comp :date :meta))))


(defn list-files-in-https-ftp-interface!
  "
  Converts the following HTML page into a list of objects sorted by date.

  Input HTML Format:
  4/25/2019  8:15 PM        <dir> 2007
  5/19/2010 11:37 PM        <dir> 2009
  5/19/2010 11:37 PM        <dir> 2010

  Output Object Format:
  ({:meta {:date '2010-05-19' :time '11:37 PM' :is-dir? true}
    :href '/pub/HEALTH_STATISTICS/NCHS/Publications/ICD10CM/2009/'
    :file-name '2009'}
   {:meta {:date '2010-05-19' :time '11:37 PM' :is-dir? true}
    :href '/pub/HEALTH_STATISTICS/NCHS/Publications/ICD10CM/2010/'
    :file-name '2010'}
   {:meta {:date '2019-04-25' :time '8:15 PM' :is-dir? true}
    :href '/pub/HEALTH_STATISTICS/NCHS/Publications/ICD10CM/2007/'
    :file-name '2007'})

  Parameters:
  - url (String): The URL of the HTML page to parse.

  Returns:
  - A sorted list of file objects.
  "
  [url]
  (let [hickory-dom
        (get-html-doc-represented-as-hickory url)

        a-tags-adjacent-with-meta
        (get-adjacent-text-nodes-and-a-tags hickory-dom)

        file-objects
        (create-file-objects a-tags-adjacent-with-meta)]
    file-objects))


(defn download-file!
  "Downloads a file from a source location and saves it to a destination location.

  This function takes two file paths, 'from' (source) and 'to' (destination), and copies the
  contents of the 'from' file to the 'to' file. It ensures that the input and output streams
  are properly closed after the file transfer is completed.

  Parameters:
  - from (String or java.io.File): The path to the source file to be downloaded.
  - to (String or java.io.File): The path to the destination file where the source file
    will be saved.

  Example:
  (download-file! \"/path/to/source/file.txt\" \"/path/to/destination/file.txt\")
  ; Downloads 'file.txt' from the source location to the destination location.

  (download-file! (java.io.File. \"/path/to/source/file.txt\") (java.io.File. \"/path/to/destination/file.txt\"))
  ; Downloads 'file.txt' using java.io.File objects as input paths."
  [from to]
  (with-open [in (io/input-stream from)
              out (io/output-stream to)]
    (io/copy in out)))



(defmethod u/*fn ::download-latest-icd10!
  ([{:as   _ctx,
     :keys [download-dest ftp-https-interface-url ftp-https-interface-domain]}]
   (let [release-dirs (->> ftp-https-interface-url
                           (list-files-in-https-ftp-interface!)
                           (remove (fn [file-object]
                                     (= "CM- Committee" (:name file-object)))))
         latest-release-dir (last release-dirs)
         latest-release-full-url (str ftp-https-interface-domain (:href latest-release-dir))
         latest-release-dir-content (list-files-in-https-ftp-interface! latest-release-full-url)]

     (.mkdirs (io/file download-dest))

     (doseq [file-obj latest-release-dir-content]
       (let [file-full-url (str ftp-https-interface-domain (:href file-obj))
             dest-path (io/file download-dest (:file-name file-obj))]
         (download-file! file-full-url dest-path)))

     {:release-version (:file-name latest-release-dir)})))

;; TODO: The FTP server at ftp.cdc.gov isn't responding, so we've temporarily switched to the FTP-HTTPS web interface.
;;
;; (ftp/with-ftp [client "ftp://anonymous:pwd@ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM"
;;                   :file-type :binary]
;;      (let [release-dirs       (->> (ftp/client-FTPFile-directories client)
;;                                    (remove (fn [^org.apache.commons.net.ftp.FTPFile f]
;;                                              (= "CM- Committee" (.getName f))))
;;                                    (sort (fn [^org.apache.commons.net.ftp.FTPFile f1
;;                                               ^org.apache.commons.net.ftp.FTPFile f2]
;;                                            (compare (.getTimestamp f2) (.getTimestamp f1)))))
;;            latest-release-dir ^org.apache.commons.net.ftp.FTPFile (first release-dirs)]
;;        (ftp/client-cd client (.getName latest-release-dir))

;;        (.mkdirs (io/file download-dest))

;;        (doseq [filename (ftp/client-file-names client)]
;;          (ftp/client-get client
;;                          filename
;;                          (format "%s/%s" download-dest filename)))
;;        {:release-version (.getName latest-release-dir)}))


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
  {:ftp-https-interface-url
   "https://ftp.cdc.gov/pub/HEALTH_STATISTICS/NCHS/Publications/ICD10CM/"

   :ftp-https-interface-domain
   "https://ftp.cdc.gov/"

   :download-dest
   "/tmp/ftr/ci_pipelines/icd10"

   :ftr-path
   "/tmp/ftr"})


(defn pipeline [args]
  (let [cfg (-> (merge config-defaults args)
                (assoc :ftr.utils.unifn.core/tracers [:ftr.logger.core/log-step]))]
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
