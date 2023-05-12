(ns ftr.writer.core
  (:require [clojure.java.io :as io]
            [ftr.utils.core]
            [cheshire.core])
  (:import [java.sql
            PreparedStatement
            ResultSet]))


(defn create-temp-tf-path! [ftr-path]
  (let [ftr-dir (ftr.utils.core/create-dir-if-not-exists! ftr-path)]
    (format "%s/%s"
            (.getAbsolutePath ftr-dir)
            (ftr.utils.core/gen-uuid))))


(defn generate-id-if-not-present [res]
  (if-not (:id res)
    (let [generated-id
          (case (:resourceType res)
            "CodeSystem"
            (:url res)

            "ValueSet"
            (:url res))
          escaped-generated-id (ftr.utils.core/escape-url generated-id)]
      (assoc res :id escaped-generated-id))
    res))


(defn generate-concept-id [concept vs]
  (assoc concept :id (ftr.utils.core/escape-url
                       (str (:system concept)  "-" (:url vs) "-" (:code concept)))))


(defn spit-tf-file! [^java.io.BufferedWriter writer cs vs c]
  (let [sorted-code-systems (sort-by :id cs)]
    (with-open [w writer]
      (doseq [cs sorted-code-systems]
        (.write w (ftr.utils.core/generate-ndjson-row (generate-id-if-not-present cs))))
      (.write w (ftr.utils.core/generate-ndjson-row (generate-id-if-not-present vs)))
      (if (coll? c) ;; TODO Smells? Move to multimethod? c - can be regular clojure collection OR PreparedStatement obj
        (doseq [c (sort-by #(format "%s-%s" (:system %) (:code %)) c)]
          (.write w (ftr.utils.core/generate-ndjson-row (generate-concept-id c vs))))
        (with-open [^PreparedStatement pstmnt c]
          (with-open [^ResultSet rset (.executeQuery pstmnt)]
            (while (.next rset)
              (.write w (-> (.getString rset "ftr_concept")
                            (cheshire.core/parse-string keyword)
                            (generate-concept-id vs)
                            (ftr.utils.core/generate-ndjson-row))))))))))


(defn write-terminology-file!
  [{:as _ctx,
    {:keys [ftr-path]} :cfg
    {:keys [value-set code-system concepts]} :extraction-result}]
  (let [value-set (update value-set :name ftr.utils.core/escape-url)
        {:keys [writer ^java.io.File file digest]}
        (ftr.utils.core/make-sha256-gzip-writer (create-temp-tf-path! ftr-path))

        _ (spit-tf-file! writer code-system value-set concepts)

        sha256
        (digest)

        renamed-file (io/file (format "%s/tf.%s.ndjson.gz" (.getParent file) sha256))]

    (.renameTo file
               renamed-file)

    {:value-set value-set
     :code-system code-system
     :terminology-file renamed-file
     :tf-sha256 sha256}))


(defn write-terminology-file [ctx]
  (write-terminology-file! ctx))
