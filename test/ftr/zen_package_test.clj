(ns ftr.zen-package-test
  (:require [ftr.core]
            [ftr.pull.core]
            [clojure.java.shell :as shell]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [zen.package]
            [matcho.core :as matcho]
            [ftr.utils.core]))


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
                            [dep-name (mk-module-dir-path root-dir-path dep-name)]))
                     deps)}))


(defn mk-module-fixture [root-dir-path module-name module-params]
  (let [module-dir-path (mk-module-dir-path root-dir-path module-name)]

    (mkdir module-dir-path)

    (zen.package/zen-init! module-dir-path)

    (spit-zrc module-dir-path (:zrc module-params))

    (spit-deps root-dir-path module-dir-path (:deps module-params))

    (git-init-commit module-dir-path)

    :done))


(defn mk-fixtures [test-dir-path deps]
  (mkdir test-dir-path)

  (doseq [module-name (keys deps)]
    (mk-module-fixture test-dir-path module-name (get deps module-name))))


(defn rm-fixtures [test-dir-path]
  (rm test-dir-path))


(def test-zen-repos
  {'test-module {:deps '#{a-lib}
                 :zrc '#{{:ns main
                          :import #{a}
                          sym {:zen/tags #{a/tag}
                               :a "a"}}}}

   'a-lib       {:deps '#{a-lib-dep b-lib}
                 :zrc '#{{:ns a
                          :import #{a-dep b}
                          tag {:zen/tags #{zen/schema zen/tag}
                               :confirms #{a-dep/tag-sch}}
                          recur-sch {:zen/tags #{zen/schema}
                                     :type zen/map
                                     :keys {:a {:confirms #{b/recur-sch}}}}}}}

   'b-lib        {:deps '#{a-lib}
                  :zrc '#{{:ns b
                           :import #{a}
                           recur-sch {:zen/tags #{zen/schema}
                                      :type zen/map
                                      :keys {:b {:confirms #{a/recur-sch}}}}}}}

   'a-lib-dep   {:deps '#{}
                 :zrc '#{{:ns a-dep
                          tag-sch
                          {:zen/tags #{zen/schema}
                           :type zen/map
                           :require #{:a}
                           :keys {:a {:type zen/string}}}}}}})


(t/deftest init-test
  (def test-dir-path "/tmp/ftr.zen-package-test")

  (rm-fixtures test-dir-path)

  (mk-fixtures test-dir-path test-zen-repos)

  (def module-dir-path (str test-dir-path "/test-module"))

  (zen.package/zen-init-deps! module-dir-path)

  (def ztx (zen.core/new-context {:package-paths [module-dir-path]}))

  (zen.core/read-ns ztx 'main)

  (t/testing "no errors in package"
    (t/is (empty? (zen.core/errors ztx)))))

