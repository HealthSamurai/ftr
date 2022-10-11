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
                            (if (vector? dep-name)
                              [(first dep-name) (second dep-name)]
                              [dep-name (mk-module-dir-path root-dir-path dep-name)])))
                     deps)}))


(defn spit-resources [root-dir-path module-dir-path resources]
  (mkdir (str module-dir-path "/resources/"))
  (doseq [[path content] resources]
    (spit (str module-dir-path "/resources/" path)
          content)))


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


(def icd10-no-header-csv-content
  "10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Accidents;10344;;1;
15062;20012;W00-X59;Other external causes of accidental injury;16003;;1;10/07/2020
11748;2001203;W50-W64;Exposure to animate mechanical forces;15062;;1;
11870;2001203W64;W64;Exposure to other and unspecified animate mechanical forces;11748;;1;
11871;2001203W640;W64.0;Exposure to other and unspecified animate mechanical forces home while engaged in sports activity;11870;;1;
11872;2001203W641;W64.00;Exposure to other and unspecified animate mechanical forces, home, while engaged in sports activity;11871;;1;
11873;2001203W641;W64.01;Exposure to other and unspecified animate mechanical forces, home, while engaged in leisure activity;11871;;1;")


(defn test-zen-repos [root-path]
  {'profile-lib {:deps #{['zen-fhir (str (System/getProperty "user.dir") "/zen.fhir/")]}
                 :resources {"icd-10.csv" icd10-no-header-csv-content}
                 :zrc #{{:ns 'profile
                         :import #{'zen.fhir}

                         'diagnosis-vs
                         {:zen/tags #{'zen.fhir/value-set}
                          :zen.fhir/version "0.5.0"
                          :uri "diagnosis-vs"
                          :ftr {:module            "ftr"
                                :source-url        (str root-path "/profile-lib/resources/icd-10.csv")
                                :ftr-path          root-path
                                :tag               "v1"
                                :source-type       :flat-table
                                :extractor-options {:format "csv"
                                                    :csv-format {:delimiter ";"
                                                                 :quote     "'"}
                                                    :header      false
                                                    :data-row    0
                                                    :mapping     {:concept {:code    {:column 2}
                                                                            :display {:column 3}}}
                                                    :code-system {:id  "icd10"
                                                                  :url "http://hl7.org/fhir/sid/icd-10"}
                                                    :value-set   {:id   "icd10"
                                                                  :name "icd10.accidents"
                                                                  :url  "http://hl7.org/fhir/ValueSet/icd-10"}}}}

                         'sch
                         '{:zen/tags #{zen/schema zen.fhir/structure-schema}
                           :zen.fhir/version "0.5.0"
                           :type zen/map
                           :keys {:diagnosis {:type zen/string
                                              :zen.fhir/value-set {:symbol diagnosis-vs}}}}}}}

   'test-module {:deps '#{profile-lib}
                 :zrc '#{{:ns main
                          :import #{profile}

                          sch {:zen/tags #{zen/schema}
                               :confirms #{profile/sch}
                               :type zen/map
                               :require #{:diagnosis}}}}}})


(t/deftest ^:kaocha/pending init-test
  (def test-dir-path "/tmp/ftr.zen-package-test")

  (rm-fixtures test-dir-path)

  (mk-fixtures test-dir-path (test-zen-repos test-dir-path))

  (def module-dir-path (str test-dir-path "/test-module"))

  (zen.package/zen-init-deps! module-dir-path)

  (def ztx (zen.core/new-context {:package-paths [module-dir-path]}))

  (zen.core/read-ns ztx 'main)

  (t/testing "no errors in package"
    (t/is (empty? (zen.core/errors ztx))))

  (t/testing "ftr extracted")

  (t/testing "vs validation works"
    (matcho/match (zen.core/validate ztx #{'main/sch} {:diagnosis "incorrect-diagnosis"})
                  {:errors [{} nil]})

    (matcho/match (zen.core/validate ztx #{'main/sch} {:diagnosis "W64.0"})
                  {:errors empty?})))
