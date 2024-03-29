(ns build
  (:require [clojure.tools.build.api :as b]))

(def version (format "1.2.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"
                            :aliases [:pipeline]}))
(def uber-file "target/ftr.jar")
(def uber-file-zen-cli "target/zen.jar")

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "zrc"]
               :target-dir class-dir})
  (b/compile-clj {:basis basis
                  :src-dirs ["src"]
                  :class-dir class-dir})

  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis basis
           :main 'ftr.cli})

  (b/uber {:class-dir class-dir
           :uber-file uber-file-zen-cli
           :basis basis
           :main 'ftr.zen-cli}))
