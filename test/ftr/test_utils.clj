(ns ftr.test-utils
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [zen.package]
            [clojure.java.io :as io]
            [org.httpkit.server]
            [ring.middleware.params]
            [ring.middleware.keyword-params])
  (:import java.io.File))


(defn sh! [& args]
  (println "$" (str/join " " args))
  (let [result (apply shell/sh args)]

    (when-not (str/blank? (:out result))
      (print (:out result)))

    (when-not (str/blank? (:err result))
      (print "!" (:err result)))

    (flush)
    result))


(defn mkdir [name] (sh! "mkdir" "-p" name))
(defn rm [& names] (apply sh! "rm" "-rf" names))

(defn git-init-commit [dir]
  (sh! "git" "add" "." :dir dir)
  (sh! "git" "commit" "-m" "\"Initial commit\"" :dir dir))


(defn mk-module-dir-path [root-dir-path module-name]
  (str root-dir-path "/" module-name))


(defn zen-ns->file-name [zen-ns]
  (-> (name zen-ns)
      (str/replace \. \/)
      (str ".edn")))


(defn spit-zrc [module-dir-path zen-namespaces]
  (mkdir (str module-dir-path "/zrc"))

  (doseq [zen-ns zen-namespaces]
    (let [file-name (zen-ns->file-name (or (get zen-ns :ns) (get zen-ns 'ns)))]
      (spit (str module-dir-path "/zrc/" file-name) zen-ns))))


(defn spit-deps [root-dir-path module-dir-path deps]
  (spit (str module-dir-path "/zen-package.edn")
        {:deps (into {}
                     (map (fn [dep-name]
                            (if (vector? dep-name)
                              [(first dep-name) (second dep-name)]
                              [dep-name (mk-module-dir-path root-dir-path dep-name)])))
                     deps)}))


(defn spit-resources [root-dir-path module-dir-path resources]
  (mkdir (str module-dir-path "/resources/"))
  (doseq [[path content] resources]
    (let [res-path (str module-dir-path "/resources/" path)]
      (io/make-parents res-path)
      (spit res-path content))))


(defn mk-module-fixture [root-dir-path module-name module-params]
  (let [module-dir-path (mk-module-dir-path root-dir-path module-name)]

    (mkdir module-dir-path)

    (zen.package/zen-init! module-dir-path)

    (spit-zrc module-dir-path (:zrc module-params))

    (spit-deps root-dir-path module-dir-path (:deps module-params))

    (spit-resources root-dir-path module-dir-path (:resources module-params))

    (git-init-commit module-dir-path)

    :done))


(defn mk-fixtures [test-dir-path deps]
  (mkdir test-dir-path)

  (doseq [[module-name module-params] deps]
    (mk-module-fixture test-dir-path module-name module-params)))


(defn rm-fixtures [test-dir-path]
  (rm test-dir-path))


(defn fs-tree->tree-map [path]
  (let [splitted-path (drop 1 (str/split path #"/"))
        tree-map (reduce
                   (fn [store path] (assoc-in store path {}))
                   {}
                   (map (fn [f] (drop 1 (str/split (str f) #"/"))) (file-seq (io/file path))))]
    (get-in tree-map splitted-path)))


(defn flatten-keys* [a ks m]
  (if (map? m)
    (reduce into (map (fn [[k v]] (flatten-keys* a (conj ks k) v)) (seq m)))
    (assoc a ks m)))


(defn flatten-keys [m] (flatten-keys* {} [] m))


(defn fs-tree->paths [m]
  (-> m
      (flatten-keys)
      (keys)
      (->> (map #(str/join File/separatorChar (map name %))))))


(defn create-zip-archive-from-fs-tree [fs-tree out-path]
  (let [zip-parent-dir (.getParentFile (io/file out-path))]
    (when-not (.exists zip-parent-dir) (.mkdirs zip-parent-dir))
    (with-open [^java.util.zip.ZipOutputStream
                zip-stream
                (-> out-path
                    (io/output-stream)
                    (java.util.zip.ZipOutputStream.))]
      (doseq [^String path (fs-tree->paths fs-tree)]
        (let [zip-entry (java.util.zip.ZipEntry. path)]
          (.putNextEntry zip-stream zip-entry)
          (io/copy "" zip-stream)
          (.closeEntry zip-stream))))))


;;;;; mock server utils ;;;;;

(def mock-server-opts {:port 7654})


(defn start-mock-server [handler & [opts]]
  (org.httpkit.server/run-server (-> handler
                                     ring.middleware.keyword-params/wrap-keyword-params
                                     ring.middleware.params/wrap-params)
                                 (merge mock-server-opts opts)))


(defn mock-server-url [& [port]]
  (format "http://localhost:%s"
          (or port (:port mock-server-opts))))
