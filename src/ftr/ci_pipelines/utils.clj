(ns ftr.ci-pipelines.utils
  (:require [ftr.utils.unifn.core :as u]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [cheshire.core :as json]
            [org.httpkit.client :as http]
            [clojure.java.io :as io])
  (:import java.io.File
           [java.util.zip ZipInputStream]))


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


(defn- generate-patch-stats-html
  [stats]
  (->> stats
       (map #(format "<code>%s</code> — %s" (first %) (second %)))
       (str/join "\n")))


(defn- generate-patch-details-html
  [errors? {:as _patch, :keys [patch-created? stats]}]
  (cond
    errors? "Aaaaa! CI pipeline spectacularly failed."
    patch-created? (format "There are updates with the following stats:\n%s"
                           (generate-patch-stats-html stats))
    :else "There are no updates."))


(defn- send-telegram-msg!
  [bot-token chat-id msg]
  (let [url (str "https://api.telegram.org/bot" bot-token "/sendMessage")]
    @(http/post url
                {:headers {"content-type" "application/json"}
                 :body (json/generate-string
                         {:chat_id chat-id
                          :text msg
                          :disable_notification true
                          :parse_mode "HTML"})})))


(defmethod u/*fn ::send-tg-notification
  [{:as ctx,
    :keys
    [patch-generation-result
     tg-bot-token tg-channel-id
     ci-run-url
     langs]
    {:keys [module]} :cfg}]
  (let [errors?             (= :error (::u/status ctx))
        results-details-msg (generate-patch-details-html errors? patch-generation-result)
        results-emoji       (if errors? "❌" "✅")
        langs-str           (if langs
                              (str
                                " "
                                (->> langs
                                     (map (fn [{:keys [lang country]}] (format "%s-%s" lang country)))
                                     (str/join ", ")))
                              "")
        final-msg           (format "%s Module: <b>%s%s</b>\n\n%s\n\nSee <a href=\"%s\">CI run</a>."
                                    results-emoji
                                    (str/upper-case module)
                                    langs-str
                                    results-details-msg
                                    ci-run-url)]
    (send-telegram-msg! tg-bot-token
                        tg-channel-id
                        final-msg)
    {}))


(defn unzip-file!
  "uncompress zip archive.
  `input` - name of zip archive to be uncompressed.
  `output` - name of folder where to output."
  [input output]
  (with-open [stream (-> input io/input-stream ZipInputStream.)]
    (loop [entry (.getNextEntry stream)]
      (if entry
        (let [save-path (str output File/separatorChar (.getName entry))
              out-file (File. save-path)]
          (if (.isDirectory entry)
            (if-not (.exists out-file)
              (.mkdirs out-file))
            (let [parent-dir (File. (.substring save-path 0 (.lastIndexOf save-path (int File/separatorChar))))]
              (when-not (.exists parent-dir) (.mkdirs parent-dir))
              (clojure.java.io/copy stream out-file)))
          (recur (.getNextEntry stream)))))))
