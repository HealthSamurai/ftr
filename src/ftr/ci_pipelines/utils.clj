(ns ftr.ci-pipelines.utils
  (:require [ftr.utils.unifn.core :as u]
            [clojure.java.shell :as shell]
            [clojure.string :as str]))


(defmethod u/*fn ::download-previous-ftr-version!
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
