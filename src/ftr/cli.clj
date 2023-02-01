(ns ftr.cli
  (:gen-class)
  (:require [ftr.core]
            [cli-matic.core]
            [zen.cli]
            [ftr.zen-package]
            [clojure.string :as str]
            [clojure.pprint]))


(defn parse-ftr-cfg [path]
  (try (read-string (slurp path))
       (catch Exception e {::error (.getMessage e)})))


(defn get-ftr-cfg [path]
  (let [current-dir (System/getProperty "user.dir")
        cfg-path    (or path (format "%s/.ftr-config.edn" current-dir))
        parsed-cfg  (parse-ftr-cfg cfg-path)]
    (if (get parsed-cfg ::error)
      parsed-cfg
      {:cfg parsed-cfg
       :cfg-path cfg-path})))

(def cli-cfg
  {:command "ftr"
   :description "FTR"
   :version "0.0.1"
   :opts []
   :subcommands [{:command "commit"
                  :description "Apply existing config to FTR"
                  :opts [{:option "config"
                          :as "Path to config file"
                          :type :string}]
                  :runs (fn [{:keys [config]}]
                          (let [current-dir (System/getProperty "user.dir")
                                cfg-path    (or config (format "%s/.ftr-config.edn" current-dir))
                                parsed-cfg  (parse-ftr-cfg cfg-path)]
                            (if-let [error (::error parsed-cfg)]
                              (println (str "\u001B[31m" error "\u001B[0m"))
                              (ftr.core/apply-cfg  {:cfg parsed-cfg
                                                    :ftr.utils.unifn.core/tracers [:ftr.logger.core/dispatch-logger]}))))}
                 {:command "tag"
                  :description "Create a tag relying on an existing config and the tag specified in it"
                  :opts [{:option "config"
                          :as "Path to config file"
                          :type :string}]
                  :runs (fn [{:keys [config]
                              [tag] :_arguments}]
                          (let [current-dir (System/getProperty "user.dir")
                                cfg-path    (or config (format "%s/.ftr-config.edn" current-dir))
                                parsed-cfg  (parse-ftr-cfg cfg-path)]
                            (if-let [error  (::error parsed-cfg)]
                              (println (str"\u001B[31m" error "\u001B[0m" ))
                              (spit cfg-path (assoc parsed-cfg
                                                    :tag      tag
                                                    :move-tag {:old-tag (:tag parsed-cfg)
                                                               :new-tag tag})))))}
                 {:command "config"
                  :description "Provide config paths"
                  :opts [{:option "config"
                          :as "Path to config file"
                          :type :string}]
                  :subcommands [{:command "set"
                                 :description "Set new key in config"
                                 :runs (fn [{:keys [config]
                                             [k v] :_arguments}]
                                         (let [{:keys  [cfg cfg-path]
                                                ::keys [error]} (get-ftr-cfg config)
                                               k-contains-dot? (str/includes? k ".")
                                               assoc-path (->> (str/split k #"\.")
                                                               (map keyword))]
                                           (if-not error
                                             (spit cfg-path (if k-contains-dot?
                                                              (assoc-in cfg assoc-path v)
                                                              (assoc cfg (keyword k) v)))
                                             (println (str"\u001B[31m" error "\u001B[0m" )))))}
                                {:command "remove"
                                 :description "Removes key from config"
                                 :runs (fn [{:keys [config]
                                             [k _v] :_arguments}]
                                         (let [{:keys  [cfg cfg-path]
                                                ::keys [error]} (get-ftr-cfg config)]
                                           (if-not error
                                             (spit cfg-path (dissoc cfg (keyword k)))
                                             (println (str"\u001B[31m" error "\u001B[0m" )))))}
                                {:command "view"
                                 :description "View config content"
                                 :runs (fn [{:keys [config]}]
                                         (let [{:keys  [cfg cfg-path]
                                                ::keys [error]} (get-ftr-cfg config)]
                                           (if-not error
                                             (do
                                               (println (format "Config content at: %s\n" cfg-path))
                                               (clojure.pprint/pprint cfg))
                                             (println (str"\u001B[31m" error "\u001B[0m" )))))}
                                {:command "create"
                                 :description "Create config from template"
                                 :opts [{:option "template"
                                         :as "Template to create config from"
                                         :type :string}
                                        {:option "source"
                                         :as "Source"
                                         :type :string}
                                        {:option "config"
                                         :as "Path to resulting config file"
                                         :type :string}]
                                 :runs (fn [{:keys [template source config]}]
                                         (let [current-dir (System/getProperty "user.dir")
                                               cfg-path    (or config (format "%s/.ftr-config.edn" current-dir))
                                               cfg (case template
                                                     "ig"
                                                     {:module      "new-module"
                                                      :source-url  source
                                                      :source-type :ig
                                                      :ftr-path    (str current-dir \/ "ftr")
                                                      :tag         "init"}

                                                     "csv"
                                                     {:module            "new-module"
                                                      :source-url        source
                                                      :ftr-path          (str current-dir \/ "ftr")
                                                      :tag               "init"
                                                      :source-type       :flat-table
                                                      :extractor-options {:format "csv"
                                                                          :csv-format      {:delimiter ";"
                                                                                            :quote     "'"}
                                                                          :header      false
                                                                          :data-row    0
                                                                          :mapping     {:concept {:code    {:column 2}
                                                                                                  :display {:column 3}}}
                                                                          :code-system {:id  "cs-id"
                                                                                        :url "cs-url"}
                                                                          :value-set   {:id   "vs-id"
                                                                                        :name "icd10.accidents"
                                                                                        :url  "vs-url"}}}

                                                     (str"\u001B[31m" "Unknown template: " template "\u001B[0m" ))]
                                           (when (map? cfg)
                                             (spit cfg-path cfg))
                                           (println "")
                                           (clojure.pprint/pprint cfg)))}]}]})


(defn -main
  [& args]
  (cli-matic.core/run-cmd args cli-cfg))
