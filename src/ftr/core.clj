(ns ftr.core
  (:require [clojure.java.io :as io]
            [ftr.extraction.core]
            [ftr.writer.core]
            [ftr.ingestion-coordinator.core]
            [ftr.utils.core]
            [ftr.utils.unifn.core :as u]
            [progrock.core :as pr]))


(defmethod u/*fn ::extract-terminology [{:as _ctx, :keys [cfg]}]
  {:extraction-result (ftr.extraction.core/extract cfg)})


(defmethod u/*fn ::write-terminology-file [ctx]
  {:write-result (ftr.writer.core/write-terminology-file ctx)})


(defmethod u/*fn ::shape-ftr-layout [{:as _ctx, :keys [cfg write-result]}]
  (let [tag (:tag cfg)
        ?new-tag (get-in cfg [:move-tag :new-tag])
        ?old-tag (get-in cfg [:move-tag :old-tag])
        module-path (format "%s/%s" (:ftr-path cfg) (:module cfg))
        tags-path (format "%s/tags" module-path)
        vs-dir-path (format "%s/vs" module-path)
        value-set-name (ftr.utils.core/escape-url (get-in write-result [:value-set :url]))
        temp-tf-file (:terminology-file write-result)
        tf-name (.getName temp-tf-file)
        temp-tf-path (.getAbsolutePath temp-tf-file)
        vs-name-path (format "%s/%s" vs-dir-path value-set-name)]

    (doseq [p [tags-path vs-dir-path vs-dir-path vs-name-path]]
      (ftr.utils.core/create-dir-if-not-exists! p))

    {:ftr-layout
     {:module-path module-path
      :tags-path tags-path
      :vs-dir-path vs-dir-path
      :vs-name-path vs-name-path
      :temp-tf-path temp-tf-path
      :tf-path (format "%s/%s" vs-name-path tf-name)
      :tf-tag-path (format "%s/tag.%s.ndjson.gz" vs-name-path tag)
      :old-tf-tag-path (when ?new-tag (format "%s/tag.%s.ndjson.gz" vs-name-path ?old-tag))
      :tag-index-path (format "%s/%s.ndjson.gz" tags-path tag)
      :new-tag-index-path (when ?new-tag (format "%s/%s.ndjson.gz" tags-path ?new-tag))}}))


(defmethod u/*fn ::feeder [{:as ctx, :keys [extraction-result pipeline-opts]
                            {:keys [ftr-path]} :cfg}]
  extraction-result
  (let [render-logs?  (get-in ctx [:cfg :pipeline-opts :render-logs?])
        statistics    (atom {:patches 0
                             :tfs     0
                             :pb (when render-logs?
                                   (pr/progress-bar (count extraction-result)))})]
    (when render-logs? (println "\n📦 Commit")) ;;TODO MOVE TO TRACER FN!!!
    (doseq [[vs-url tf] extraction-result]
      (let [{:as result,
             {patch-created? :patch-created?} :patch-generation-result}
            (u/*apply [::write-terminology-file
                       ::shape-ftr-layout
                       :ftr.ingestion-coordinator.core/ingest-terminology-file]
                      (assoc ctx :extraction-result tf))]
        (swap! statistics (fn [old-state]
                            (cond-> old-state
                              patch-created?
                              (update :patches inc)
                              :always
                              (update :tfs inc))))
        (when render-logs?
          (let [pb (-> statistics deref :pb)]
            (pr/print pb
                           {:format (format ":progress/:total   :percent%% [:bar] Processing ValueSet: %s" vs-url)})
            (swap! statistics assoc :pb (pr/tick pb))))))

    (when render-logs?
      (pr/print (pr/done (-> statistics deref :pb)) {:format ":progress/:total   :percent% [:bar] Processing ValueSet: "})
      (println (str \newline "Results: "))
      (println (format "    Terminology Files created: \033[0;1m%s\033[22m" (-> statistics deref :tfs)))
      (println (format "    Patches created: \033[0;1m%s\033[22m" (-> statistics deref :patches)))
      (println (format "    FTR Repository size: \033[0;1m%s MB\033[22m" (int (/ (ftr.utils.core/psize ftr-path) 1000000)))))))


(defn apply-cfg [{:as cfg, :keys [source-type]}]
  (condp = source-type
    :flat-table
    (u/*apply [::extract-terminology
               ::write-terminology-file
               ::shape-ftr-layout
               :ftr.ingestion-coordinator.core/ingest-terminology-file] {:cfg cfg})

    :ig
    (u/*apply [::extract-terminology
               ::feeder] {:cfg cfg})))


(comment
  (def usr-cfg
    {:module            "fhir"
     :ftr-path          "/tmp/ftr"
     :tag               "r3"
     :source-type       :ig
     :source-url "/tmp/r3"})

  (def usr-cfg-2
    {:module            "fhir"
     :ftr-path          "/tmp/ftr"
     :move-tag {:old-tag "r3"
                :new-tag "r4"}
     :tag               "r4"
     :source-type       :ig
     :source-url "/tmp/r4"})

  (time (do (apply-cfg usr-cfg)
            :done))

  (time (do (apply-cfg usr-cfg-2)
            :done))

  )
