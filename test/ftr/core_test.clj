(ns ftr.core-test
  (:require [ftr.core :as sut]
            [ftr.pull.core :as pull-sut]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [matcho.core :as matcho]
            [ftr.utils.core]
            [ftr.test-utils :as test-utils]))


(defn fs-tree->tree-map [path]
  (reduce
    (fn [store path] (assoc-in store path {}))
    {}
    (map (fn [f] (drop 1 (str/split (str f) #"/"))) (file-seq (io/file path)))))



(t/deftest generate-repository-layout-from-csv-source
  (def csv-test-env-cfg {:csv-source-initial           "/tmp/ftr-fixtures/icd10initial.csv"
                         :csv-source-updated           "/tmp/ftr-fixtures/icd10updated.csv"
                         :csv-source-tag               "/tmp/ftr-fixtures/icd10newtag.csv"
                         :ftr-path                     "/tmp/ftr"
                         :expected-tf-sha256           "9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f"
                         :expected-tf-filename         "tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz"
                         :expected-updated-tf-sha256   "9371c021fe55404a053be98e0e4c3efdcf7db1564e00755fdd3a565cf21240e9"
                         :expected-updated-tf-filename "tf.9371c021fe55404a053be98e0e4c3efdcf7db1564e00755fdd3a565cf21240e9.ndjson.gz"
                         :expected-tag-tf-sha256       "4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff"
                         :expected-tag-tf-filename     "tf.4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff.ndjson.gz"
                         :expected-patch-filename      "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.9371c021fe55404a053be98e0e4c3efdcf7db1564e00755fdd3a565cf21240e9.ndjson.gz"
                         :expected-patch2-filename     "patch.9371c021fe55404a053be98e0e4c3efdcf7db1564e00755fdd3a565cf21240e9.4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff.ndjson.gz"})


  (def csv-user-cfg {:module            "icd10"
                     :source-url        (:csv-source-initial csv-test-env-cfg)
                     :ftr-path          (:ftr-path csv-test-env-cfg)
                     :tag               "v1"
                     :source-type       :flat-table
                     :extractor-options {:format "csv"
                                         :csv-format      {:delimiter ";"
                                                           :quote     "'"}
                                         :header      false
                                         :data-row    0
                                         :mapping     {:concept {:code    {:column 2}
                                                                 :display {:column 3}}}
                                         :code-system {:id  "icd10"
                                                       :url "http://hl7.org/fhir/sid/icd-10"}
                                         :value-set   {:id   "icd10"
                                                       :name "icd10.accidents"
                                                       :url  "http://hl7.org/fhir/ValueSet/icd-10"}}})


  (defn prepare-test-env! [{:as _cfg, :keys [csv-source-initial
                                             csv-source-updated
                                             csv-source-tag
                                             ftr-path]}]
    (let [fixture-file (io/file csv-source-initial)
          fixture-file-2 (io/file csv-source-updated)
          fixture-file-3 (io/file csv-source-tag)]

      (io/make-parents fixture-file)
      (spit
        fixture-file
        "10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Accidents;10344;;1;
15062;20012;W00-X59;Other external causes of accidental injury;16003;;1;10/07/2020")

      (io/make-parents fixture-file-2)
      (spit
        fixture-file-2
        "10343;766;AA;loh and mortality;;;1;
10343;666;X;morbidity and mortality;;;1;
10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Updated accidents;10344;;1;")

      (io/make-parents fixture-file-3)
      (spit
        fixture-file-3
        "10343;666;X;morbidity and mortality;;;1;")

      (ftr.utils.core/rmrf ftr-path)))

  (defn clean-up-test-env! [{:as _cfg, :keys [csv-source-initial
                                              csv-source-updated
                                              csv-source-tag
                                              ftr-path]}]
    (ftr.utils.core/rmrf csv-source-initial)
    (ftr.utils.core/rmrf csv-source-updated)
    (ftr.utils.core/rmrf csv-source-tag)
    (ftr.utils.core/rmrf ftr-path))

  (t/testing "User provides config for CSV"
    (prepare-test-env! csv-test-env-cfg)

    (let [{:as user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          csv-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          tf-tag-file-name
          (format "tag.%s.ndjson.gz" tag)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {module
           {"tags"
            {(format "%s.ndjson.gz" tag) {}}
            "vs"
            {value-set-name
             {(:expected-tf-filename csv-test-env-cfg) {}
              tf-tag-file-name                     {}}}}}))

      (t/testing "sees tag ingex content"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz
           (format "%s/icd10/tags/%s.ndjson.gz"
                   (:ftr-path csv-test-env-cfg)
                   (:tag user-cfg)))
          [{:name (format "%s.%s" module value-set-name) :hash (:expected-tf-sha256 csv-test-env-cfg)}
           nil?]))

      (t/testing "sees terminology tag file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz
           (format "%s/icd10/vs/%s/%s"
                   ftr-path
                   value-set-name
                   tf-tag-file-name))
          [{:tag tag :hash (:expected-tf-sha256 csv-test-env-cfg)}
           nil?])))

    )

  (t/testing "User provides updated config for CSV"
    (let [{:as user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          (assoc csv-user-cfg :source-url (:csv-source-updated csv-test-env-cfg))

          value-set-name (ftr.utils.core/escape-url value-set-name)

          tf-tag-file-name
          (format "tag.%s.ndjson.gz" tag)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees updated repository layout, new tf sha is correct, patch file created"
        (matcho/match
         ftr-tree
         {module
          {"tags"
           {(format "%s.ndjson.gz" tag) {}}
           "vs"
           {value-set-name
            {(:expected-tf-filename csv-test-env-cfg)         {}
             (:expected-updated-tf-filename csv-test-env-cfg) {}
             (:expected-patch-filename csv-test-env-cfg)      {}
             tf-tag-file-name                             {}}}}})
        )

      (t/testing "sees tag ingex content"
        (matcho/match
         (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/tags/%s.ndjson.gz" (:ftr-path csv-test-env-cfg) (:tag user-cfg)))
         [{:name (format "%s.%s" module value-set-name) :hash (:expected-updated-tf-sha256 csv-test-env-cfg)}
          nil?]))

      (t/testing "sees terminology tag file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/vs/%s/%s" ftr-path value-set-name tf-tag-file-name))
          [{:tag tag :hash (:expected-updated-tf-sha256 csv-test-env-cfg)}
           {:from (:expected-tf-sha256 csv-test-env-cfg) :to (:expected-updated-tf-sha256 csv-test-env-cfg)}
           nil?]))

      (t/testing "sees terminology patch file"
        (matcho/match
          (sort-by :code (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/vs/%s/%s" ftr-path value-set-name (:expected-patch-filename csv-test-env-cfg))))
          [{:name value-set-name}
           {:code "AA" :op "add"}
           {:code "V01-X59" :op "update"}
           {:code "W00-X59" :op "remove"}
           {:code "X" :op "add"}
           nil?]))))

  (clean-up-test-env! csv-test-env-cfg))


(t/deftest generate-repository-layout-from-ig-source
  (def ig-test-env-cfg
    {:ig-source-initial           "test/fixture/dehydrated.core/node_modules"
     :ig-source-updated           "test/fixture/dehydrated.mutated.core/node_modules"
     :ftr-path                    "/tmp/igftr"
     :vs1-name                    "http:--hl7.org-fhir-ValueSet-administrative-gender"
     :vs2-name                    "http:--hl7.org-fhir-ValueSet-codesystem-content-mode"
     :expected-tf1-sha256         "cf470821d2fc2aa31221ecce73e2239837a735a4bf2a94e80eba17985c00b162"
     :expected-tf1-filename       "tf.cf470821d2fc2aa31221ecce73e2239837a735a4bf2a94e80eba17985c00b162.ndjson.gz"
     :expected-updated-tf1-sha256 "b035cc432cc6044eff1dbee4b71a4136068b762160fe30fdf181dd92638dd1b3"
     :expected-updated-tf1-filename "tf.b035cc432cc6044eff1dbee4b71a4136068b762160fe30fdf181dd92638dd1b3.ndjson.gz"
     :expected-tf1-patch-filename "patch.cf470821d2fc2aa31221ecce73e2239837a735a4bf2a94e80eba17985c00b162.b035cc432cc6044eff1dbee4b71a4136068b762160fe30fdf181dd92638dd1b3.ndjson.gz"
     :expected-tf2-sha256   "612a2239b16753161f2328a212ccea619de2a32553651540b89490d4d87904fc"
     :expected-tf2-filename "tf.612a2239b16753161f2328a212ccea619de2a32553651540b89490d4d87904fc.ndjson.gz"
     :expected-updated-tf2-sha256 "30aebaf54e705e685c26ce28f8f6f5c2a603647166a16e8fc60b6efd37d9c15c"
     :expected-updated-tf2-filename "tf.30aebaf54e705e685c26ce28f8f6f5c2a603647166a16e8fc60b6efd37d9c15c.ndjson.gz"
     :expected-tf2-patch-filename "patch.612a2239b16753161f2328a212ccea619de2a32553651540b89490d4d87904fc.30aebaf54e705e685c26ce28f8f6f5c2a603647166a16e8fc60b6efd37d9c15c.ndjson.gz"
     })


  (def ig-user-cfg {:module      "dehydrated"
                    :source-url  (:ig-source-initial ig-test-env-cfg)
                    :ftr-path    (:ftr-path ig-test-env-cfg)
                    :tag         "v1"
                    :source-type :ig})

  (ftr.utils.core/rmrf (:ftr-path ig-test-env-cfg))


  (let [{:as user-cfg, :keys [module ftr-path tag]}
        ig-user-cfg

        {:keys [vs1-name vs2-name]}
        ig-test-env-cfg

        tf-tag-file-name
        (format "tag.%s.ndjson.gz" tag)

        _
        (sut/apply-cfg {:cfg user-cfg})


        ftr-tree
        (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

    (t/testing "sees generated repository layout, tf sha is correct"
      (matcho/match
       ftr-tree
       {module
        {"tags"
         {(format "%s.ndjson.gz" tag) {}}
         "vs"
         {vs1-name
          {(:expected-tf1-filename ig-test-env-cfg) {}
           tf-tag-file-name                     {}}
          vs2-name
          {(:expected-tf2-filename ig-test-env-cfg) {}
           tf-tag-file-name                     {}}}}}))

    (t/testing "sees tag ingex content"
      (matcho/match
       (ftr.utils.core/parse-ndjson-gz
        (format "%s/%s/tags/%s.ndjson.gz"
                (:ftr-path ig-test-env-cfg)
                (:module user-cfg)
                (:tag user-cfg)))
       [{:name (format "%s.%s" module vs1-name)
         :hash (:expected-tf1-sha256 ig-test-env-cfg)}
        {:name (format "%s.%s" module vs2-name)
         :hash (:expected-tf2-sha256 ig-test-env-cfg)}
        nil?]))

    (t/testing "sees terminology tag files"
      (t/testing (format "for %s vs" vs1-name)
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/%s"
                                                  ftr-path
                                                  (:module ig-user-cfg)
                                                  vs1-name
                                                  tf-tag-file-name))
          [{:tag tag
            :hash (:expected-tf1-sha256 ig-test-env-cfg)}
           nil?]))

      (t/testing (format "for %s vs" vs1-name)
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/%s"
                                                  ftr-path
                                                  (:module ig-user-cfg)
                                                  vs2-name
                                                  tf-tag-file-name))
          [{:tag tag
            :hash (:expected-tf2-sha256 ig-test-env-cfg)}
           nil?]))))


  (t/testing "User provides updated config for IG"
    (let [{:as user-cfg, :keys [module ftr-path tag]}
          (assoc ig-user-cfg :source-url (:ig-source-updated ig-test-env-cfg))

          {:keys [vs1-name vs2-name]}
          ig-test-env-cfg

          tf-tag-file-name
          (format "tag.%s.ndjson.gz" tag)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees updated repository layout, new tf sha is correct, patch file created"
        (matcho/match
          ftr-tree
          {module
           {"tags"
            {(format "%s.ndjson.gz" tag) {}}
            "vs"
            {vs1-name
             {(:expected-tf1-filename ig-test-env-cfg)         {}
              (:expected-updated-tf1-filename ig-test-env-cfg) {}
              (:expected-tf1-patch-filename ig-test-env-cfg)   {}
              tf-tag-file-name                                  {}}
             vs2-name
             {(:expected-tf2-filename ig-test-env-cfg)         {}
              (:expected-updated-tf2-filename ig-test-env-cfg) {}
              (:expected-tf2-patch-filename ig-test-env-cfg)   {}
              tf-tag-file-name                                  {}}}}})
        )

      (t/testing "sees tag ingex content"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz
            (format "%s/%s/tags/%s.ndjson.gz"
                    (:ftr-path ig-test-env-cfg)
                    (:module user-cfg)
                    (:tag user-cfg)))
          [{:name (format "%s.%s" module vs1-name) :hash (:expected-updated-tf1-sha256 ig-test-env-cfg)}
           {:name (format "%s.%s" module vs2-name) :hash (:expected-updated-tf2-sha256 ig-test-env-cfg)}
           nil?]))

      (t/testing "sees terminology tag file"
        (t/testing (format "%s vs" vs1-name)
          (matcho/match
            (ftr.utils.core/parse-ndjson-gz
              (format "%s/%s/vs/%s/%s"
                      ftr-path
                      (:module user-cfg)
                      vs1-name
                      tf-tag-file-name))
            [{:tag tag :hash (:expected-updated-tf1-sha256 ig-test-env-cfg)}
             {:from (:expected-tf1-sha256 ig-test-env-cfg) :to (:expected-updated-tf1-sha256 ig-test-env-cfg)}
             nil?]))

        (t/testing (format "%s vs" vs2-name)
          (matcho/match
            (ftr.utils.core/parse-ndjson-gz
              (format "%s/%s/vs/%s/%s"
                      ftr-path
                      (:module user-cfg)
                      vs2-name
                      tf-tag-file-name))
            [{:tag tag :hash (:expected-updated-tf2-sha256 ig-test-env-cfg)}
             {:from (:expected-tf2-sha256 ig-test-env-cfg) :to (:expected-updated-tf2-sha256 ig-test-env-cfg)}
             nil?])))

      (t/testing "sees terminology patch file"
        (t/testing (format "%s vs" vs1-name)
          (matcho/match
            (sort-by :code (ftr.utils.core/parse-ndjson-gz
                             (format "%s/%s/vs/%s/%s"
                                     ftr-path
                                     (:module user-cfg)
                                     vs1-name
                                     (:expected-tf1-patch-filename ig-test-env-cfg))))
            [{:name vs1-name}
             {:op "remove" :code "other"}
             nil?]))

        (t/testing (format "%s vs" vs2-name)
          (matcho/match
            (sort-by :code (ftr.utils.core/parse-ndjson-gz
                             (format "%s/%s/vs/%s/%s"
                                     ftr-path
                                     (:module user-cfg)
                                     vs2-name
                                     (:expected-tf2-patch-filename ig-test-env-cfg))))
            [{:name vs2-name}
             {:op "update" :code "not-present"
              :definition "!None! of the concepts defined by the code system are included in the code system resource."}
             {:op "add" :code "test-content"}
             nil?])))

      (t/testing "Pull-initialize operation"
        (pull-sut/init user-cfg {:patch-plan-file-name "ig-plan"})

        (t/testing "patch plan file generated correctly"
          (matcho/match
            (ftr.utils.core/parse-ndjson-gz "/tmp/ftr-bulk-patch-plans/ig-plan.ndjson.gz")
            [{:resourceType "CodeSystem" :name "AdministrativeGender"}
             {:resourceType "ValueSet"   :name "AdministrativeGender"}
             {:code "female"}
             {:code "male"}
             {:code "unknown"}
             {:resourceType "CodeSystem" :name "CodeSystemContentMode"}
             {:resourceType "ValueSet"   :name "CodeSystemContentMode"}
             {:code "complete"}
             {:code "example"}
             {:code "fragment"}
             {:code "not-present"}
             {:code "supplement"}
             {:code "test-content"}
             nil?])))))


  (ftr.utils.core/rmrf (:ftr-path ig-test-env-cfg)))

(t/deftest update-test
  (let [env      {:ig-source-initial     "test/fixture/dehydrated.core/node_modules"
                  :ig-source-updated1    "test/fixture/dehydrated.mutated.core/node_modules"
                  :ig-source-updated2    "test/fixture/dehydrated.mutated.core2/node_modules"
                  :ig-source-updated3    "test/fixture/dehydrated.mutated.core3/node_modules"
                  :update-plan-name      "update-plan"
                  :update-plan-name-path "/tmp/ftr-bulk-patch-plans/update-plan.ndjson.gz"
                  :ftr-path              "/tmp/igftr"}
        user-cfg {:module      "dehydrated"
                  :source-url  (:ig-source-initial env)
                  :ftr-path    (:ftr-path env)
                  :tag         "v1"
                  :source-type :ig}

        _          (ftr.utils.core/rmrf (:ftr-path env))
        _          (sut/apply-cfg {:cfg user-cfg})
        client-cfg (merge user-cfg
                          {:tag       "v2"
                           :tag-index (ftr.utils.core/parse-ndjson-gz
                                        (format "%s/%s/tags/%s.ndjson.gz"
                                                (:ftr-path env)
                                                (:module user-cfg)
                                                (:tag user-cfg)))
                           :tag-index-hash (slurp (format "%s/%s/tags/%s.hash"
                                                          (:ftr-path env)
                                                          (:module user-cfg)
                                                          (:tag user-cfg)))})]

    (t/testing "do pull/update on unchanged master ftr state"
      (t/testing "no update plan has been created"
        (matcho/match
          (pull-sut/migrate (-> client-cfg
                                (assoc :update-plan-name (:update-plan-name env) :tag (:tag user-cfg))))
          nil?)))

    (sut/apply-cfg {:cfg (assoc user-cfg :source-url (:ig-source-updated1 env))})
    (sut/apply-cfg {:cfg (merge user-cfg {:source-url (:ig-source-updated2 env)})})
    (sut/apply-cfg {:cfg (merge user-cfg {:source-url (:ig-source-updated3 env)
                                          :tag        "v2"})})


    (t/testing "do pull/update"
      (matcho/match
        (update (pull-sut/migrate (assoc client-cfg :update-plan-name (:update-plan-name env))) :remove-plan sort)
        {:patch-plan (:update-plan-name-path env)
         :remove-plan
         ["administrative-gender"
          "administrative-gender"
          "http:--hl7.org-fhir-administrative-gender-http:--hl7.org-fhir-ValueSet-administrative-gender-female"
          "http:--hl7.org-fhir-administrative-gender-http:--hl7.org-fhir-ValueSet-administrative-gender-male"
          "http:--hl7.org-fhir-administrative-gender-http:--hl7.org-fhir-ValueSet-administrative-gender-other"
          "http:--hl7.org-fhir-administrative-gender-http:--hl7.org-fhir-ValueSet-administrative-gender-unknown"
          nil?]}))


    (t/testing "sees bulk update plan file"
      (matcho/match
        (ftr.utils.core/parse-ndjson-gz (:update-plan-name-path env))
        [{:resourceType "CodeSystem" :name "v3.NullFlavor"}
         {:resourceType "ValueSet"   :name "v3.NullFlavor"}
         {:code "ASKU"}
         {:code "DER"}
         {:code "INV"}
         {:code "MSK"}
         {:code "NA"}
         {:code "NASK"}
         {:code "NAV"}
         {:code "NAVU"}
         {:code "NI"}
         {:code "NINF"}
         {:code "NP"}
         {:code "OTH"}
         {:code "PINF"}
         {:code "QS"}
         {:code "TRC"}
         {:code "UNC"}
         {:code "UNK"}
         nil?]))))


(t/deftest multiple-cs-in-terminology-file
  (let [ig-folder "/tmp/ig/"

        ftr-path "/tmp/csigftr"

        ->json cheshire.core/generate-string
        <-json #(cheshire.core/parse-string % keyword)
        _ (ftr.utils.core/rmrf ig-folder)

        cs1 {:resourceType "CodeSystem"
             :id "gender-cs-id"
             :url "gender-cs-url"
             :status "active"
             :content "complete"
             :concept [{:code "male" :display "Male"}
                       {:code "female" :display "Female"}
                       {:code "other" :display "Other"}
                       {:code "unknown" :display "Unknown"}]}

        cs2 {:resourceType "CodeSystem"
             :id "shortgender-cs-id"
             :url "shortgender-cs-url"
             :status "active"
             :content "complete"
             :concept [{:code "m" :display "M"}
                       {:code "f" :display "F"}]}

        vs {:resourceType "ValueSet"
            :id "gender-vs-id"
            :url "gender-vs"
            :status "active"
            :compose {:include [{:system "gender-cs-url"}
                                {:system "shortgender-cs-url"}]}}

        ig-manifest {:name "ig.core"
                     :version "0.0.1"
                     :type "ig.core"
                     :title "ig.core"
                     :description "ig.core"
                     :author "hs"
                     :url "dehydrated"}

        resources (-> {"gender-codesystem.json" cs1
                       "shortgender-codesystem.json" cs2
                       "gender-valueset.json" vs
                       "package.json" ig-manifest}
                      (update-keys #(str ig-folder %))
                      (update-vals ->json))

        _ (doseq [[path content] resources] (do (io/make-parents path)
                                                (spit path content)))

        user-cfg {:module      "dehydrated"
                  :source-url  ig-folder
                  :ftr-path    ftr-path
                  :tag         "v1"
                  :source-type :ig}
        _ (ftr.utils.core/rmrf ftr-path)
        _ (sut/apply-cfg {:cfg user-cfg})
        _ (sut/apply-cfg {:cfg user-cfg})

        _ (t/testing "ftr shape correct"
            (matcho/match
              (get-in (fs-tree->tree-map ftr-path) (drop 1 (str/split ftr-path #"/")))
              {"dehydrated"
               {"vs"
                {"gender-vs"
                 {"tf.d4fe7700488a6f482fb97b12091fe1cc3b5096045aa853df07ede853c17f2530.ndjson.gz" {}
                  "tag.v1.ndjson.gz" {}}}
                "tags" {"v1.ndjson.gz" {}}}}))

        _ (t/testing "gender-vs v1.tag file doesn't have duplicate from-to entry"
            (matcho/match
              (ftr.utils.core/parse-ndjson-gz (str ftr-path "/dehydrated" "/vs" "/gender-vs" "/tag.v1.ndjson.gz"))
              [{:hash
                "d4fe7700488a6f482fb97b12091fe1cc3b5096045aa853df07ede853c17f2530"
                :tag "v1"}
               nil?]))

        client-cfg
        (merge user-cfg
               {:tag-index (ftr.utils.core/parse-ndjson-gz
                             (format "%s/%s/tags/%s.ndjson.gz"
                                     ftr-path
                                     (:module user-cfg)
                                     (:tag user-cfg)))})

        resources (update-in resources [(str ig-folder "shortgender-codesystem.json")]
                             (fn [content]
                               (-> content
                                   <-json
                                   (update :concept conj {:code "o" :display "O"})
                                   ->json)))

        _ (doseq [[path content] resources] (do (io/make-parents path)
                                                (spit path content)))

        _ (sut/apply-cfg {:cfg (assoc user-cfg :tag "v2")})

        _ (t/testing "ftr shape correct"
            (matcho/match
              (get-in (fs-tree->tree-map ftr-path) (drop 1 (str/split ftr-path #"/")))
              {"dehydrated"
               {"vs"
                {"gender-vs"
                 {"tf.935b647b83da220fd0d351634a98a40dd53efa2e26461159e512e15e89a19e7b.ndjson.gz" {}
                  "tf.d4fe7700488a6f482fb97b12091fe1cc3b5096045aa853df07ede853c17f2530.ndjson.gz" {}
                  "tag.v1.ndjson.gz" {}}}
                "tags" {"v1.ndjson.gz" {} "v1.hash" {}}}}))

        update-plan-name "update-plan"
        update-plan-path (str "/tmp/ftr-bulk-patch-plans" \/ update-plan-name ".ndjson.gz")

        _ (t/testing "pull/migrate create correct update plan, remove plan remains empty"
            (matcho/match
              (update (pull-sut/migrate (assoc client-cfg :tag "v2" :update-plan-name update-plan-name)) :remove-plan sort)
              {:patch-plan update-plan-path
               :remove-plan empty?})

            (t/testing "update plan is correct"
              (matcho/match
                (ftr.utils.core/parse-ndjson-gz update-plan-path)
                [{:resourceType "CodeSystem" :id "gender-cs-id"}
                 {:resourceType "CodeSystem" :id "shortgender-cs-id"}
                 {:resourceType "ValueSet"   :id "gender-vs-id"}
                 {:resourceType "Concept"    :code "o"}
                 nil?])))]))


(t/deftest tag-index-hash-side-file-creation
  (let [csv-source-initial           "/tmp/ftr-fixtures/icd10initial.csv"
        csv-source-updated           "/tmp/ftr-fixtures/icd10updated.csv"
        ftr-path                     "/tmp/ftr"

        csv-user-cfg {:module            "icd10"
                      :source-url        csv-source-initial
                      :ftr-path          ftr-path
                      :tag               "v1"
                      :source-type       :flat-table
                      :extractor-options {:format "csv"
                                          :csv-format      {:delimiter ";"
                                                            :quote     "'"}
                                          :header      false
                                          :data-row    0
                                          :mapping     {:concept {:code    {:column 2}
                                                                  :display {:column 3}}}
                                          :code-system {:id  "icd10"
                                                        :url "http://hl7.org/fhir/sid/icd-10"}
                                          :value-set   {:id   "icd10"
                                                        :name "icd10.accidents"
                                                        :url  "http://hl7.org/fhir/ValueSet/icd-10"}}}
        value-set-name (ftr.utils.core/escape-url (get-in csv-user-cfg [:extractor-options :value-set :url]))

        _prepared-test-env (let [fixture-file-1 (io/file csv-source-initial)
                                 fixture-file-2 (io/file csv-source-updated)]

                             (io/make-parents fixture-file-1)
                             (spit
                               fixture-file-1
"10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Accidents;10344;;1;
15062;20012;W00-X59;Other external causes of accidental injury;16003;;1;10/07/2020")

                             (spit fixture-file-2
"10343;766;AA;loh and mortality;;;1;
10343;666;X;morbidity and mortality;;;1;
10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Updated accidents;10344;;1;"))

        _ (t/testing "Newly generated FTR has correct layout"
            (sut/apply-cfg {:cfg csv-user-cfg})

            (matcho/match
              (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))
              {(:module csv-user-cfg)
               {"tags" {(str (:tag csv-user-cfg) ".ndjson.gz") {}
                        (str (:tag csv-user-cfg) ".hash") {}}
                "vs" {value-set-name {}}}})

            (t/testing "Tag index hash is correct"
              (matcho/match
                (try (slurp (format "%s/%s/tags/%s.hash" ftr-path (:module csv-user-cfg) (:tag csv-user-cfg)))
                     (catch Exception _ :file-not-exists))
                "68430c6867f892e215a267e1a8c819cc97e1bb746f6fce2517ce1356a7c17526\n")))
        _ (t/testing "Updated FTR has correct layout"
            (sut/apply-cfg {:cfg (assoc csv-user-cfg :source-url csv-source-updated)})

            (matcho/match
              (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))
              {(:module csv-user-cfg)
               {"tags" {(str (:tag csv-user-cfg) ".ndjson.gz") {}
                        (str (:tag csv-user-cfg) ".hash") {}}
                "vs" {value-set-name {}}}})

            (t/testing "Updated tag index hash is correct"
              (matcho/match
                (try (slurp (format "%s/%s/tags/%s.hash" ftr-path (:module csv-user-cfg) (:tag csv-user-cfg)))
                     (catch Exception _ :file-not-exists))
                "e3b60170e660d9ffd98868d0ba02456c50318aa574b2cb471bd91226f1fe02f9\n")))]))

;; I just decided to stop describing the test flow as a let bindings sequence.
;; Sorry for that inconvenience.

(t/deftest migration-to-appropriate-tags-model
  (def test-env-prefix "/tmp/tag-model-migration-test/")
  (def ftr-path (str test-env-prefix "ftr"))
  (def vs-url "http://hl7.org/fhir/ValueSet/icd-10")
  (def module "icd10")
  (def escaped-vs-url (ftr.utils.core/escape-url vs-url))
  (def test-env-cfg {:ftr-path ftr-path
                     :module module
                     :vs-url vs-url
                     :escaped-vs-url escaped-vs-url
                     :path-to-vs (format "%s/%s/vs/%s/" ftr-path module escaped-vs-url)
                     :csv-source-v1-write-path (str test-env-prefix "/ftr-fixtures/icd10_v1.csv")
                     :csv-source-v2-write-path (str test-env-prefix "/ftr-fixtures/icd10_v2.csv")
                     :csv-source-v3-write-path (str test-env-prefix "/ftr-fixtures/icd10_v3.csv")
                     :csv-source-v4-write-path (str test-env-prefix "/ftr-fixtures/icd10_v4.csv")
                     :csv-source-v5-write-path (str test-env-prefix "/ftr-fixtures/icd10_v5.csv")
                     :csv-source-v6-write-path (str test-env-prefix "/ftr-fixtures/icd10_v6.csv")
                     :csv-sources-content [[(str test-env-prefix "/ftr-fixtures/icd10_v1.csv")
"10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Accidents;10344;;1;
15062;20012;W00-X59;Other external causes of accidental injury;16003;;1;10/07/2020"]
                                           [(str test-env-prefix "/ftr-fixtures/icd10_v2.csv")
"10343;766;AA;gen and mortality;;;1;
10343;666;X;morbidity and mortality;;;1;
10344;20;XX;External causes of morbidity and mortality;;;1;
16003;2001;V01-X59;Updated accidents;10344;;1;"]
                                           [(str test-env-prefix "/ftr-fixtures/icd10_v3.csv")
"10343;766;AA;gen and mortality;;;1;
10343;666;X;morbidity and mortality;;;1;
10344;20;XX;Updated External causes of morbidity and mortality;;;1;"]
                                           [(str test-env-prefix "/ftr-fixtures/icd10_v4.csv")
"10343;766;AA;gen and mortality;;;1;
10343;666;X;Updated morbidity and mortality;;;1;
10344;20;XX;Updated External causes of morbidity and mortality;;;1;"]
                                           [(str test-env-prefix "/ftr-fixtures/icd10_v5.csv")
"10343;766;AA;gen and mortality;;;1;
10343;666;X;Updated2x morbidity and mortality;;;1;
10344;20;XX;Updated External causes of morbidity and mortality;;;1;"]
                                           [(str test-env-prefix "/ftr-fixtures/icd10_v6.csv")
"10343;766;AA;gen and mortality;;;1;
10343;666;X;Updated2x morbidity and mortality;;;1;
10344;20;XX;Updated2x External causes of morbidity and mortality;;;1;"]]})

  (def ftr-user-cfg
    {:module            module
     :source-url        (:csv-source-v1-write-path test-env-cfg)
     :ftr-path          ftr-path
     :tag               "dev"
     :source-type       :flat-table
     :extractor-options {:format "csv"
                         :csv-format      {:delimiter ";"
                                           :quote     "'"}
                         :header      false
                         :data-row    0
                         :mapping     {:concept {:code    {:column 2}
                                                 :display {:column 3}}}
                         :code-system {:id  "icd10"
                                       :url "http://hl7.org/fhir/sid/icd-10"}
                         :value-set   {:id   "icd10"
                                       :name "icd10.accidents"
                                       :url  (:vs-url test-env-cfg)}}})

  ;; Should these functions really be variadics?

  (defn clean-up-test-env!
    ([] (clean-up-test-env! test-env-prefix))
    ([test-env-prefix]
     (ftr.utils.core/rmrf test-env-prefix)))

  (defn prepare-test-env!
    ([] (prepare-test-env! test-env-cfg))
    ([{:as _test-env-cfg,
       :keys [csv-sources-content]}]
     (clean-up-test-env!)

     (doseq [[write-path content] csv-sources-content]
       (let [f (io/file write-path)
             parent-dir (.getParentFile (io/file write-path))]
         (when-not (.exists parent-dir) (.mkdirs parent-dir))
         (spit f content)))))

  (t/testing "Test environment preparation. . ."
    (prepare-test-env!))

  (t/testing "Initial FTR shape is correct?"
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg ftr-user-cfg}) )

    (t/testing "FTR shape is correct"
      (matcho/match
        (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
        {"icd10"
         {"vs"
          {"http:--hl7.org-fhir-ValueSet-icd-10"
           {"tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz"
            {},
            "tag.dev.ndjson.gz" {}}},
          "tags" {"dev.ndjson.gz" {}, "dev.hash" {}}}})))

  (t/testing "Updated FTR shape is correct?"
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg (assoc ftr-user-cfg :source-url (:csv-source-v2-write-path test-env-cfg))}) )

    (t/testing "FTR shape is correct"
      (matcho/match
        (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
        {"icd10"
         {"vs"
          {"http:--hl7.org-fhir-ValueSet-icd-10"
           {"tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz" {}
            "tf.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
            "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
            "tag.dev.ndjson.gz" {}}}
          "tags" {"dev.ndjson.gz" {} "dev.hash" {}}}})))

  (t/testing "Merge `dev` tag -> `prod`, i.e. fix actual hash for `dev` tag into `prod` tag"
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg (-> ftr-user-cfg
                               (assoc :tag "prod")
                               (assoc :merge {:from "dev"})
                               (dissoc :source-url :source-type))}))

    (t/testing "FTR shape is correct"
      (t/is
        (= (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
           {"icd10"
            {"vs"
             {"http:--hl7.org-fhir-ValueSet-icd-10"
              {"tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz" {}
               "tf.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.dev.ndjson.gz" {}
               "tag.prod.ndjson.gz" {}}}
             "tags" {"dev.ndjson.gz"  {} "dev.hash"  {}
                     "prod.ndjson.gz" {} "prod.hash" {}}}})))

    (t/testing "Content of `tag.prod.ndjson.gz` is adequate and contains same hash in header as in `tag.dev.ndjson.gz`"
      (let [[{actual-hash-for-dev-tag :hash}] (ftr.utils.core/parse-ndjson-gz (format "%s/tag.dev.ndjson.gz" (:path-to-vs test-env-cfg)))]
        (matcho/match
         (try (ftr.utils.core/parse-ndjson-gz (format "%s/tag.prod.ndjson.gz" (:path-to-vs test-env-cfg)))
              (catch Exception _ :parse-error))
         [{:tag "prod" :hash (partial = actual-hash-for-dev-tag)}
          nil?]))))

  (t/testing "Try to merge `dev` tag -> `prod` once again, but `dev` tag remains in the same state, so actually nothing should happen."
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg (-> ftr-user-cfg
                               (assoc :tag "prod")
                               (assoc :merge {:from "dev"})
                               (dissoc :source-url :source-type))}))

    (t/testing "FTR shape is correct"
      (t/is
        (= (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
           {"icd10"
            {"vs"
             {"http:--hl7.org-fhir-ValueSet-icd-10"
              {"tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz" {}
               "tf.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.dev.ndjson.gz" {}
               "tag.prod.ndjson.gz" {}}}
             "tags" {"dev.ndjson.gz"  {} "dev.hash"  {}
                     "prod.ndjson.gz" {} "prod.hash" {}}}})))

    (t/testing "Content of `tag.prod.ndjson.gz` is adequate and contains same hash in header as in `tag.dev.ndjson.gz`"
      (let [[{actual-hash-for-dev-tag :hash}] (ftr.utils.core/parse-ndjson-gz (format "%s/tag.dev.ndjson.gz" (:path-to-vs test-env-cfg)))]
        (matcho/match
          (try (ftr.utils.core/parse-ndjson-gz (format "%s/tag.prod.ndjson.gz" (:path-to-vs test-env-cfg)))
               (catch Exception _ :parse-error))
          [{:tag "prod" :hash (partial = actual-hash-for-dev-tag)}
           nil?]))))

  (t/testing "Pushing 2 increments to dev tag"
    (sut/apply-cfg {:cfg (assoc ftr-user-cfg :source-url (:csv-source-v3-write-path test-env-cfg))})
    (sut/apply-cfg {:cfg (assoc ftr-user-cfg :source-url (:csv-source-v4-write-path test-env-cfg))}))

  (t/testing "Merge `dev` tag -> `prod`, i.e. fix actual hash for `dev` tag into `prod` tag"
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg (-> ftr-user-cfg
                               (assoc :tag "prod")
                               (assoc :merge {:from "dev"})
                               (dissoc :source-url :source-type))}))

    (t/testing "FTR shape is correct"
      (t/is
        (= (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
           {"icd10"
            {"vs"
             {"http:--hl7.org-fhir-ValueSet-icd-10"
              {"tf.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}
               "patch.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.ndjson.gz" {}
               "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.dev.ndjson.gz" {}
               "tf.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.prod.ndjson.gz" {}
               "tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz" {}
               "patch.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}
               "tf.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.ndjson.gz" {}
               "patch.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}}}
             "tags"
             {"dev.ndjson.gz" {}
              "prod.ndjson.gz" {}
              "prod.hash" {}
              "dev.hash" {}}}})))

    (t/testing "Content of `tag.prod.ndjson.gz` is adequate and contains same hash in header as in `tag.dev.ndjson.gz`"
      (let [[{actual-hash-for-dev-tag :hash}] (ftr.utils.core/parse-ndjson-gz (format "%s/tag.dev.ndjson.gz" (:path-to-vs test-env-cfg)))]
        (matcho/match
          (try (ftr.utils.core/parse-ndjson-gz (format "%s/tag.prod.ndjson.gz" (:path-to-vs test-env-cfg)))
               (catch Exception _ :parse-error))
          [{:tag "prod" :hash (partial = actual-hash-for-dev-tag)}
           {:from "85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95"
            :to   "2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596"}
           nil?]))))

  (t/testing "Commit 1 increment to `dev` tag, 1 increment to `prod` tag."
    (sut/apply-cfg {:cfg (assoc ftr-user-cfg :source-url (:csv-source-v5-write-path test-env-cfg))})
    (sut/apply-cfg {:cfg (assoc ftr-user-cfg :source-url (:csv-source-v6-write-path test-env-cfg) :tag "prod")}))

  (t/testing "Merge `dev` tag -> `prod`, `prod` remain in manually incremented state, i.e. fix actual hash for `dev` tag into `prod` tag"
    (t/testing "FTR Commit..."
      (sut/apply-cfg {:cfg (-> ftr-user-cfg
                               (assoc :tag "prod")
                               (assoc :merge {:from "dev"})
                               (dissoc :source-url :source-type))}))

    (t/testing "FTR shape is correct"
      (t/is
        (= (test-utils/fs-tree->tree-map (:ftr-path test-env-cfg))
           {"icd10"
            {"vs"
             {"http:--hl7.org-fhir-ValueSet-icd-10"
              {"patch.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.8ca2012655de54427192af18cc6877fe5ee28eb5f5f9f6f640cedc864e31c2ea.ndjson.gz" {}
               "tf.1135a9d0f57ea81fd2a69adc8512c5e02cdfca1fc2209516a9bc1bd2d8636e44.ndjson.gz" {}
               "tf.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}
               "tf.8ca2012655de54427192af18cc6877fe5ee28eb5f5f9f6f640cedc864e31c2ea.ndjson.gz" {}
               "patch.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.ndjson.gz" {}
               "patch.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.dev.ndjson.gz" {}
               "patch.1135a9d0f57ea81fd2a69adc8512c5e02cdfca1fc2209516a9bc1bd2d8636e44.8ca2012655de54427192af18cc6877fe5ee28eb5f5f9f6f640cedc864e31c2ea.ndjson.gz" {}
               "tf.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.ndjson.gz" {}
               "tag.prod.ndjson.gz" {}
               "tf.9fbc28de6731686e42faff3e3daedbe26e4b0fa885ec0a692501089f6310c4f.ndjson.gz" {}
               "patch.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.1135a9d0f57ea81fd2a69adc8512c5e02cdfca1fc2209516a9bc1bd2d8636e44.ndjson.gz" {}
               "patch.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}
               "tf.36f883301654c5ae97cfe16b5f6398fc4944b4eace8dc558d4c30d2607a92f02.ndjson.gz" {}
               "patch.85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95.2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596.ndjson.gz" {}}}
             "tags"
             {"dev.ndjson.gz" {}
              "prod.ndjson.gz" {}
              "prod.hash" {}
              "dev.hash" {}}}})))

    (t/testing "Content of `tag.prod.ndjson.gz` is adequate and contains same hash in header as in `tag.dev.ndjson.gz`"
      (let [[{actual-hash-for-dev-tag :hash}] (ftr.utils.core/parse-ndjson-gz (format "%s/tag.dev.ndjson.gz" (:path-to-vs test-env-cfg)))]
        (matcho/match
          (try (ftr.utils.core/parse-ndjson-gz (format "%s/tag.prod.ndjson.gz" (:path-to-vs test-env-cfg)))
               (catch Exception _ :parse-error))
          [{:tag "prod" :hash (partial = actual-hash-for-dev-tag)}
           {:from "85f36b7a0ec605ca230895cc7ae2752b3771e7b540cbccfd721d5c79bcd32e95"
            :to   "2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596"}
           {:from "2f0557093c2d24574ebc5ff5648f711f5b8530e4b13008003ef57a076af87596"
            :to "1135a9d0f57ea81fd2a69adc8512c5e02cdfca1fc2209516a9bc1bd2d8636e44"}
           {:from "1135a9d0f57ea81fd2a69adc8512c5e02cdfca1fc2209516a9bc1bd2d8636e44"
            :to actual-hash-for-dev-tag}
           nil?]))))


  (t/testing "Test environment clean-up"
    (clean-up-test-env!))

  )


(t/deftest serialized-objects-array-source-type-test
  (def soa-test-env-cfg {:ftr-path "/tmp/soa/ftr"
                         :soa-source "test/fixture/yaml/external-care-team-relationship.yaml"})


  (def soa-user-cfg {:module "soa"
                     :source-url (:soa-source soa-test-env-cfg)
                     :ftr-path (:ftr-path soa-test-env-cfg)
                     :tag "prod"
                     :source-type :serialized-objects-array
                     :extractor-options {:format "yaml"
                                         :mapping {:concept {:code {:path [:code]}
                                                             :display {:path [:display]}}}
                                         :code-system {:id "soa"
                                                       :url "http://soa/cs"}
                                         :value-set {:id "soa"
                                                     :url "http://soa/vs"}}})
  (t/testing "Clean up test-env"
    (ftr.utils.core/rmrf (:ftr-path soa-test-env-cfg)))

  (t/testing "User provides config for SOA"
    (let [{:as user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          soa-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {"soa"
           {"vs"
            {"http:--soa-vs"
             {"tf.92be9f413299bc56f798f6855a3f018b73f676e353e5faae6b51ebe8d946777b.ndjson.gz"
              {},
              "tag.prod.ndjson.gz" {}}},
            "tags" {"prod.ndjson.gz" {}, "prod.hash" {}}}}))

      (t/testing "sees terminology tag file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz
            "/tmp/soa/ftr/soa/tags/prod.ndjson.gz")
          [{:hash
            "92be9f413299bc56f798f6855a3f018b73f676e353e5faae6b51ebe8d946777b"
            :name "soa.http:--soa-vs"}
           nil?]))

      (t/testing "sees terminology file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz
            "/tmp/soa/ftr/soa/vs/http:--soa-vs/tf.92be9f413299bc56f798f6855a3f018b73f676e353e5faae6b51ebe8d946777b.ndjson.gz")
          [{:resourceType "CodeSystem"
            :url "http://soa/cs"
            :valueSet "http://soa/vs"}
           {:resourceType "ValueSet"
            :url "http://soa/vs"}
           {:code "CAREGIVER"
            :display "Caregiver"
            :id "http:--soa-cs-http:--soa-vs-CAREGIVER"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "CHILD"
            :display "Children"
            :id "http:--soa-cs-http:--soa-vs-CHILD"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "HOMPART"
            :display "Partner(s)"
            :id "http:--soa-cs-http:--soa-vs-HOMPART"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "NMTH"
            :display "Parents"
            :id "http:--soa-cs-http:--soa-vs-NMTH"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "SIGOTHR"
            :display "Significant Other"
            :id "http:--soa-cs-http:--soa-vs-SIGOTHR"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "SPS"
            :display "Spouse"
            :id "http:--soa-cs-http:--soa-vs-SPS"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           {:code "STPPRN"
            :display "Step-Parent(s)"
            :id "http:--soa-cs-http:--soa-vs-STPPRN"
            :resourceType "Concept"
            :system "http://soa/cs"
            :valueset ["http://soa/vs"]}
           nil?]))))

  (t/testing "Clean up test-env"
    (ftr.utils.core/rmrf (:ftr-path soa-test-env-cfg))))


(t/deftest soa-ndjson-test
  (def soa-ndjson-test-env-cfg
    {:ftr-path   "/tmp/soa-ndjson/ftr"
     :soa-source "test/fixture/ndjson/snomed-subset.ndjson"})


  (def soa-ndjson-user-cfg {:module            "soa-ndjson"
                            :source-url        (:soa-source soa-ndjson-test-env-cfg)
                            :ftr-path          (:ftr-path soa-ndjson-test-env-cfg)
                            :tag               "prod"
                            :source-type       :serialized-objects-array
                            :extractor-options {:format      "ndjson"
                                                :mapping     {:concept {:code      {:path [:code]}
                                                                        :display   {:path [:display]}
                                                                        :ancestors {:path [:ancestors]}}}
                                                :code-system {:description  "SNOMEDCT US tiny subset"
                                                              :content      "complete"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "CodeSystem"
                                                              :status       "active"
                                                              :id           "snomedct"
                                                              :valueSet     "http://snomed.info/sct"
                                                              :url          "http://snomed.info/sct"}
                                                :value-set   {:compose      {:include [{:system "http://snomed.info/sct"}]}
                                                              :description  "SNOMEDCT US tiny subset"
                                                              :id           "snomedct"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "ValueSet"
                                                              :status       "active"
                                                              :url          "http://snomed.info/sct"}}})

  (t/testing "Clean up test-env"
    (ftr.utils.core/rmrf (:ftr-path soa-ndjson-test-env-cfg)))

  (t/testing "User provides config for ndjson SOA"
    (let [{:as                                user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          soa-ndjson-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {module
           {"vs"
            {value-set-name
             {"tf.387d11fec384fda8314fca79530f3895df068a2fbbe6300cf094f4845861ca11.ndjson.gz" {}
              (format "tag.%s.ndjson.gz" tag)                                                 {}}}
            "tags" {(format "%s.ndjson.gz" tag) {}
                    (format "%s.hash" tag)      {}}}}))

      (t/testing "sees terminology tag file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/tags/%s.ndjson.gz" ftr-path module tag))
          [{:hash
            "387d11fec384fda8314fca79530f3895df068a2fbbe6300cf094f4845861ca11"
            :name "soa-ndjson.http:--snomed.info-sct"}
           nil?]))

      (t/testing "sees terminology file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/tf.387d11fec384fda8314fca79530f3895df068a2fbbe6300cf094f4845861ca11.ndjson.gz"
                                                  ftr-path
                                                  module
                                                  value-set-name))
          [{:id           "snomedct"
            :name         "SNOMEDCT"
            :resourceType "CodeSystem"}
           {:id           "snomedct"
            :name         "SNOMEDCT"
            :resourceType "ValueSet"}
           {:code "10000006" :display "Radiating chest pain (finding)"}
           {:code "1010235008" :display "Pain of left breast (finding)"}
           {:code "1010237000" :display "Pain of right breast (finding)"}
           {:code "102587001" :display "Acute chest pain (finding)"}
           {:code "102588006" :display "Chest wall pain (finding)"}
           {:code "102589003" :display "Atypical chest pain (finding)"}
           {:code "103015000" :display "Thoracic nerve root pain (finding)"}
           {:code    "1075631000119107"
            :display "Pain of right sternoclavicular joint (finding)"}
           {:code    "1075651000119101"
            :display "Pain of left sternoclavicular joint (finding)"}
           {:code "13057000" :display "Pleuropericardial chest pain (finding)"}
           {:code "135876005" :display "Mastalgia of puberty (finding)"}
           {:code "136791000119103" :display "Chronic thoracic back pain (finding)"}
           {:code    "15743681000119106"
            :display "Pain of bilateral shoulder blades (finding)"}
           {:code "15960061000119102"
            :display
            "Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)"}
           {:code "15960141000119102"
            :display
            "Angina co-occurrent and due to coronary arteriosclerosis (disorder)"}
           {:code "15960341000119104"
            :display
            "Unstable angina due to arteriosclerosis of coronary artery bypass graft of transplanted heart (disorder)"}
           {:code "15960381000119109"
            :display
            "Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"}
           {:code "15960461000119105"
            :display
            "Unstable angina due to arteriosclerosis of autologous arterial coronary artery bypass graft (disorder)"}
           {:code "15960541000119107"
            :display
            "Unstable angina due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"}
           {:code "15960581000119102"
            :display
            "Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"}
           {:code "15960661000119107"
            :display
            "Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"}
           {:code "161972006" :display "Central chest pain (finding)"}
           {:code "161973001" :display "Anterior chest wall pain (finding)"}
           {:code "161974007" :display "Parasternal pain (finding)"}
           {:code "161977000" :display "Costal margin chest pain (finding)"}
           {:code    "16754391000119100"
            :display "Stable angina due to coronary arteriosclerosis (disorder)"}
           {:code "19057007" :display "Status anginosus (disorder)"}
           {:code "194823009" :display "Acute coronary insufficiency (disorder)"}
           {:code "194828000" :display "Angina (disorder)"}
           {:code "200430001" :display "Breastfeeding painful (finding)"}
           {:code "202478007" :display "Sternoclavicular joint pain (finding)"}
           {:code "20793008" :display "Scapulalgia (finding)"}
           {:code "21470009" :display "Syncope anginosa (disorder)"}
           {:code "2237002" :display "Pleuritic pain (finding)"}
           {:code "225566008" :display "Ischemic chest pain (finding)"}
           {:code "230597003" :display "Intercostal post-herpetic neuralgia (disorder)"}
           {:code    "230648001"
            :display "Abdominal cutaneous nerve entrapment syndrome (disorder)"}
           {:code "233819005" :display "Stable angina (disorder)"}
           {:code "233821000" :display "New onset angina (disorder)"}
           {:code "233845001" :display "Cardiac syndrome X (finding)"}
           {:code "237453001" :display "Cyclical mastalgia (finding)"}
           {:code "237454007" :display "Non-cyclical mastalgia (finding)"}
           {:code "237455008" :display "Trigger point mastalgia (finding)"}
           {:code "237477007" :display "Pain associated with breast implant (disorder)"}
           {:code "239175003" :display "Post-thoracotomy pain syndrome (finding)"}
           {:code "247350000" :display "Sore nipple (finding)"}
           {:code "247389006" :display "Intercostal neuralgia (finding)"}
           {:code "247415009" :display "Painful lactation (finding)"}
           {:code "267981009" :display "Pain in thoracic spine (finding)"}
           {:code "274664007" :display "Chest pain on breathing (finding)"}
           {:code "274668005" :display "Non-cardiac chest pain (finding)"}
           {:code "279019008" :display "Central crushing chest pain (finding)"}
           {:code "279035001" :display "Acute thoracic back pain (finding)"}
           {:code "279036000" :display "Myofascial pain syndrome of thorax (disorder)"}
           {:code "279037009" :display "Thoracic segmental dysfunction (finding)"}
           {:code "279038004" :display "Thoracic back pain (finding)"}
           {:code "279048002" :display "Internal mammary artery syndrome (finding)"}
           {:code "279054001" :display "Pain when milk coming in (finding)"}
           {:code "281245003" :display "Musculoskeletal chest pain (finding)"}
           {:code "285385002" :display "Left sided chest pain (finding)"}
           {:code "285386001" :display "Right sided chest pain (finding)"}
           {:code "285389008" :display "Upper chest pain (finding)"}
           {:code "297217002" :display "Rib pain (finding)"}
           {:code "298254008" :display "Thoracic facet joint pain (finding)"}
           {:code "29857009" :display "Chest pain (finding)"}
           {:code "298579007" :display "Pain on movement of thoracic spine (finding)"}
           {:code "298731003" :display "Pain of sternum (finding)"}
           {:code "300995000" :display "Exercise-induced angina (disorder)"}
           {:code "314116003" :display "Post infarct angina (disorder)"}
           {:code "315025001" :display "Refractory angina (disorder)"}
           {:code "315248002" :display "Intractable breast pain (finding)"}
           {:code "315250005" :display "Persistent mastalgia (finding)"}
           {:code "3368006" :display "Dull chest pain (finding)"}
           {:code "34791000119103" :display "Chest pain due to pericarditis (finding)"}
           {:code "35928006" :display "Nocturnal angina (disorder)"}
           {:code "36859004" :display "Esophageal chest pain (finding)"}
           {:code "371030007" :display "Squeezing chest pain (finding)"}
           {:code "371806006" :display "Progressive angina (disorder)"}
           {:code "371807002" :display "Atypical angina (disorder)"}
           {:code "371808007"
            :display
            "Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)"}
           {:code "371809004"
            :display
            "Recurrent angina following placement of coronary artery stent (disorder)"}
           {:code "371810009"
            :display
            "Recurrent angina status post coronary artery bypass graft (disorder)"}
           {:code    "371811008"
            :display "Recurrent angina status post rotational atherectomy (disorder)"}
           {:code "371812001"
            :display
            "Recurrent angina status post directional coronary atherectomy (disorder)"}
           {:code    "41321000119101"
            :display "Myofascial pain syndrome of thoracic spine (disorder)"}
           {:code "41334000" :display "Angina, class II (disorder)"}
           {:code    "425991004"
            :display "Cervicothoracic segmental dysfunction (finding)"}
           {:code "426396005" :display "Cardiac chest pain (finding)"}
           {:code "429559004" :display "Typical angina (disorder)"}
           {:code "444227004" :display "Acute postthoracotomy pain syndrome (finding)"}
           {:code "4557003" :display "Preinfarction syndrome (disorder)"}
           {:code "4568003" :display "Retrosternal pain (finding)"}
           {:code "53430007" :display "Pain of breast (finding)"}
           {:code "59021001" :display "Angina decubitus (disorder)"}
           {:code "59139008" :display "Crushing chest pain (finding)"}
           {:code "608831003" :display "Thoracic discogenic pain (disorder)"}
           {:code "61490001" :display "Angina, class I (disorder)"}
           {:code "713829001" :display "Postinfective intercostal neuralgia (disorder)"}
           {:code    "713839007"
            :display "Intercostal neuralgia as late effect of trauma (disorder)"}
           {:code "71884009" :display "Precordial pain (finding)"}
           {:code "735940001" :display "Pain of intercostal space (finding)"}
           {:code "774134007" :display "Pain of right shoulder blade (finding)"}
           {:code "774135008" :display "Pain of left shoulder blade (finding)"}
           {:code    "791000119109"
            :display "Angina due to type 2 diabetes mellitus (disorder)"}
           {:code "7921000119104" :display "Anterior pleuritic pain (finding)"}
           {:code "81953000" :display "Chest pain on exertion (finding)"}
           {:code "83264000" :display "Epidemic pleurodynia (disorder)"}
           {:code "85284003" :display "Angina, class III (disorder)"}
           {:code "866043000" :display "Neuralgia of twelfth thoracic nerve (finding)"}
           {:code "87343002" :display "Prinzmetal angina (disorder)"}
           {:code "89323001" :display "Angina, class IV (disorder)"}
           {:code "89638008" :display "Xiphodynia (finding)"}
           {:code "89874002" :display "Xiphoidalgia syndrome (disorder)"}
           {:code "9267009" :display "Chest pain at rest (finding)"}
           {:code "95421005" :display "Intercostal myalgia (finding)"}
           {:code "97001000119106" :display "Localized chest pain (finding)"}
           {:code    "98611000119104"
            :display "Chronic post-thoracotomy pain syndrome (finding)"}
           nil?]))))

  (t/testing "Clean up test-env"
    (ftr.utils.core/rmrf (:ftr-path soa-ndjson-test-env-cfg))))


(t/deftest ftr-from-ftr-test
  (t/testing "Preparing test env"
    (def base-ftr-test-env-cfg
      {:ftr-path "/tmp/ftr-from-ftr"
       :soa-source "test/fixture/ndjson/snomed-subset.ndjson"})

    (def base-ftr-user-cfg {:module            "snomedbase"
                            :source-url        (:soa-source base-ftr-test-env-cfg)
                            :ftr-path          (:ftr-path base-ftr-test-env-cfg)
                            :tag               "prod"
                            :source-type       :serialized-objects-array
                            :extractor-options {:format      "ndjson"
                                                :mapping     {:concept {:code      {:path [:code]}
                                                                        :display   {:path [:display]}
                                                                        :ancestors {:path [:ancestors]}}}
                                                :code-system {:description  "SNOMEDCT US tiny subset"
                                                              :content      "complete"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "CodeSystem"
                                                              :status       "active"
                                                              :id           "snomedct"
                                                              :valueSet     "http://snomed.info/sct"
                                                              :url          "http://snomed.info/sct"}
                                                :value-set   {:compose      {:include [{:system "http://snomed.info/sct"}]}
                                                              :description  "SNOMEDCT US tiny subset"
                                                              :id           "snomedct"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "ValueSet"
                                                              :status       "active"
                                                              :url          "http://snomed.info/sct"}}})

    (def base-ftr-test-other-env-cfg
      {:ftr-path "/tmp/ftr-from-ftr"
       :soa-source "test/fixture/ndjson/icd10-subset.ndjson"})

    (def base-ftr-user-other-cfg {:module            "icd10base"
                                  :source-url        (:soa-source base-ftr-test-other-env-cfg)
                                  :ftr-path          (:ftr-path base-ftr-test-other-env-cfg)
                                  :tag               "prod"
                                  :source-type       :serialized-objects-array
                                  :extractor-options {:format      "ndjson"
                                                      :hierarchy    true
                                                      :mapping     {:concept {:code      {:path [:code]}
                                                                              :display   {:path [:definition]}
                                                                              :hierarchy {:path [:hierarchy]}}}
                                                      :code-system {:id  "icd10"
                                                                    :url "http://hl7.org/fhir/sid/icd-10"}
                                                      :value-set   {:id  "icd10"
                                                                    :url "http://hl7.org/fhir/ValueSet/icd-10"}}})

    (def subset-ftr-test-env-cfg
      {:ftr-path "/tmp/ftr-from-ftr"
       :source-ftr (format "/tmp/ftr-from-ftr/%s" (:module base-ftr-user-cfg))})

    (def subset-ftr-user-cfg
      {:module            "snomedsubset"
       :source-url        (:source-ftr subset-ftr-test-env-cfg)
       :ftr-path          (:ftr-path subset-ftr-test-env-cfg)
       :tag               "prod"
       :source-type       :ftr
       :extractor-options {:target-tag "prod"
                           :value-set
                           {:compose      {:include [{:valueSet ["http://snomed.info/sct"]
                                                      :filter [{:op       "is-a"
                                                                :property "concept"
                                                                :value    "102587001"}]}]}
                            :description  "SNOMEDCT US micro subset"
                            :id           "microsnomedct"
                            :name         "MICROSNOMEDCT"
                            :resourceType "ValueSet"
                            :status       "active"
                            :url          "http://microsnomed.info/sct"}}})

    (def subset-multiple-ftrs-test-env-cfg
      {:ftr-path "/tmp/ftr-from-ftr"
       :source-ftrs [(format "/tmp/ftr-from-ftr/%s" (:module base-ftr-user-cfg))
                     (format "/tmp/ftr-from-ftr/%s" (:module base-ftr-user-other-cfg))]})

    (def subset-multiple-ftrs-user-cfg
      {:module            "snomed-icd10-subsets"
       :source-urls       (:source-ftrs subset-multiple-ftrs-test-env-cfg)
       :ftr-path          (:ftr-path subset-multiple-ftrs-test-env-cfg)
       :tag               "prod"
       :source-type       :ftr
       :extractor-options {:target-tags (into {}
                                              (map (fn [url] [url "prod"]))
                                              (:source-ftrs subset-multiple-ftrs-test-env-cfg))
                           :value-set
                           {:compose      {:include [{:valueSet ["http://snomed.info/sct"]
                                                      :filter   [{:op       "is-a"
                                                                  :property "concept"
                                                                  :value    "102587001"}]}
                                                     {:valueSet ["http://hl7.org/fhir/ValueSet/icd-10"]
                                                      :filter   [{:op       "descendent-of"
                                                                  :property "concept"
                                                                  :value    "A19"}]}]}
                            :description  "SNOMEDCTUS and ICD10 micro subsets"
                            :id           "micro-snomedct-icd10"
                            :name         "MICRO-SNOMEDCT-ICD10"
                            :resourceType "ValueSet"
                            :status       "active"
                            :url          "http://micro-snomed-and-icd10.info/sct"}}})


    (defn clean-up-test-env! [{:as _cfg, :keys [ftr-path]}]
      (ftr.utils.core/rmrf ftr-path))

    (clean-up-test-env! base-ftr-test-env-cfg))


  (t/testing "Generating base FTR’s"
    (let [{:as                                user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          base-ftr-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {module
           {"vs"
            {value-set-name
             {"tf.387d11fec384fda8314fca79530f3895df068a2fbbe6300cf094f4845861ca11.ndjson.gz" {}
              (format "tag.%s.ndjson.gz" tag)                                                 {}}}
            "tags" {(format "%s.ndjson.gz" tag) {}
                    (format "%s.hash" tag)      {}}}})))

    (let [{:as                                user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          base-ftr-user-other-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
         ftr-tree
         {module
          {"vs"
           {value-set-name
            {"tf.e851a2bd2bcc49ac6091d14ce2e61cb516ea25475372c651ef66acffcd71b3cd.ndjson.gz" {}
             (format "tag.%s.ndjson.gz" tag)                                                 {}}}
           "tags" {(format "%s.ndjson.gz" tag) {}
                   (format "%s.hash" tag)      {}}}}))))

  (t/testing "Generating new subset ftr using base ftr"
    (let [{:as                                user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          subset-ftr-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {"snomedbase"
           {"vs"
            {"http:--snomed.info-sct"
             {"tf.387d11fec384fda8314fca79530f3895df068a2fbbe6300cf094f4845861ca11.ndjson.gz"
              {}
              "tag.prod.ndjson.gz" {}}}
            "tags" {"prod.ndjson.gz" {} "prod.hash" {}}}
           "snomedsubset"
           {"vs"
            {"http:--microsnomed.info-sct"
             {"tag.prod.ndjson.gz" {}
              "tf.df1384e9eeda8164bc3e73ef7586d41500a952744e7a6799f141fd40ebb7f894.ndjson.gz"
              {}}}
            "tags" {"prod.ndjson.gz" {} "prod.hash" {}}}}))

      (t/testing "sees generated tf file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/tf.df1384e9eeda8164bc3e73ef7586d41500a952744e7a6799f141fd40ebb7f894.ndjson.gz"
                                                  (:ftr-path subset-ftr-test-env-cfg)
                                                  (:module subset-ftr-user-cfg)
                                                  value-set-name))
          [{:resourceType "CodeSystem" :url "http://snomed.info/sct"}
           {:resourceType "ValueSet" :url "http://microsnomed.info/sct"}
           {:code "102587001" :display "Acute chest pain (finding)"}
           {:code "279035001" :display "Acute thoracic back pain (finding)"}
           {:code "444227004" :display "Acute postthoracotomy pain syndrome (finding)"}
           nil?]))))

  (t/testing "Generating new subset ftr using multiple ftrs"
    (let [{:as                                user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options}
          subset-multiple-ftrs-user-cfg

          value-set-name (ftr.utils.core/escape-url value-set-name)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {"snomedbase"
           {}
           "icd10base"
           {}
           "snomed-icd10-subsets"
           {"vs"
            {"http:--micro-snomed-and-icd10.info-sct"
             {"tf.951f5cfd0fc0b5bb0443ac5d8b9449223f7e8f27f541ebb8bc7d1239c8bf4b17.ndjson.gz"
              {}
              "tag.prod.ndjson.gz" {}}}
            "tags" {"prod.ndjson.gz" {} "prod.hash" {}}}}))

      (t/testing "sees generated tf file"
        (matcho/match
         (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/tf.951f5cfd0fc0b5bb0443ac5d8b9449223f7e8f27f541ebb8bc7d1239c8bf4b17.ndjson.gz"
                                                  (:ftr-path subset-multiple-ftrs-test-env-cfg)
                                                  (:module subset-multiple-ftrs-user-cfg)
                                                  value-set-name))
         [{:resourceType "CodeSystem" :url "http://hl7.org/fhir/sid/icd-10"}
          {:resourceType "CodeSystem" :url "http://snomed.info/sct"}
          {:resourceType "ValueSet"   :url "http://micro-snomed-and-icd10.info/sct"}
          {:code "A19.0" :display "Acute miliary tuberculosis of a single specified site"}
          {:code "A19.1" :display "Acute miliary tuberculosis of multiple sites"}
          {:code "A19.2" :display "Acute miliary tuberculosis, unspecified"}
          {:code "A19.8" :display "Other miliary tuberculosis"}
          {:code "A19.9" :display "Miliary tuberculosis, unspecified"}
          {:code "102587001" :display "Acute chest pain (finding)"}
          {:code "279035001" :display "Acute thoracic back pain (finding)"}
          {:code "444227004" :display "Acute postthoracotomy pain syndrome (finding)"}
          nil?]))))

  )


(t/deftest supplement-ftr-test
  (def soa-ndjson-test-env-cfg
    {:ftr-path   "/tmp/soa-ndjson/ftr"
     :soa-source "test/fixture/ndjson/snomed-subset.ndjson"})

  (def soa-ndjson-user-cfg {:module            "microsnomed"
                            :source-url        (:soa-source soa-ndjson-test-env-cfg)
                            :ftr-path          (:ftr-path soa-ndjson-test-env-cfg)
                            :tag               "prod"
                            :source-type       :serialized-objects-array
                            :extractor-options {:format      "ndjson"
                                                :mapping     {:concept {:code      {:path [:code]}
                                                                        :display   {:path [:display]}
                                                                        :ancestors {:path [:ancestors]}}}
                                                :code-system {:description  "SNOMEDCT US tiny subset"
                                                              :content      "complete"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "CodeSystem"
                                                              :status       "active"
                                                              :id           "snomedct"
                                                              :valueSet     "http://snomed.info/sct"
                                                              :url          "http://snomed.info/sct"}
                                                :value-set   {:compose      {:include [{:system "http://snomed.info/sct"}]}
                                                              :description  "SNOMEDCT US tiny subset"
                                                              :id           "snomedct"
                                                              :name         "SNOMEDCT"
                                                              :resourceType "ValueSet"
                                                              :status       "active"
                                                              :url          "http://snomed.info/sct"}}})

  (def sup-dep-test-env-cfg {:source-url "test/fixture/supplementdependant.core/"
                             :ftr-path "/tmp/supdep/ftr"})

  (def sup-dep-user-cfg {:module            "supplementdependant"
                         :source-url        (:source-url sup-dep-test-env-cfg)
                         :ftr-path          (:ftr-path sup-dep-test-env-cfg)
                         :tag               "prod"
                         :source-type       :ig
                         :extractor-options {:supplements [{:source-url (:ftr-path soa-ndjson-test-env-cfg) :module "microsnomed" :tag "prod"}]}})

  (t/testing "Clean up test-env"
    (ftr.utils.core/rmrf (:ftr-path soa-ndjson-test-env-cfg)))

  (t/testing "Generating supplement ftr"
    (sut/apply-cfg {:cfg soa-ndjson-user-cfg}))

  (t/testing "Generating supplement-dependant ftr"
    (let [{:as user-cfg, :keys [ftr-path]}
          sup-dep-user-cfg

          value-set-name "http:--sup-dependant.info"

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]

      (t/testing "sees generated repository layout, tf sha is correct"
        (matcho/match
          ftr-tree
          {"supplementdependant"
           {"vs"
            {"http:--snomed.info-sct" nil?
             "http:--sup-dependant.info"
             {"tag.prod.ndjson.gz" {}
              "tf.a1e056f1bc60caa8bceb1ec5a20aa4e496628e76200da2a48dc5072bfe31ffa5.ndjson.gz"
              {}}}
            "tags" {"prod.ndjson.gz" {} "prod.hash" {}}}}))

      (t/testing "generated tag index file contains only one valueset - http://sup-dependant.info"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/tags/%s.ndjson.gz"
                                                  (:ftr-path sup-dep-test-env-cfg)
                                                  (:module sup-dep-user-cfg)
                                                  (:tag sup-dep-user-cfg)))
          [{:hash "a1e056f1bc60caa8bceb1ec5a20aa4e496628e76200da2a48dc5072bfe31ffa5"
            :name "supplementdependant.http:--sup-dependant.info"}
           nil?]))

      (t/testing "sees generated tf file"
        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/%s/vs/%s/tf.a1e056f1bc60caa8bceb1ec5a20aa4e496628e76200da2a48dc5072bfe31ffa5.ndjson.gz"
                                                  (:ftr-path sup-dep-test-env-cfg)
                                                  (:module sup-dep-user-cfg)
                                                  value-set-name))
          [{:resourceType "CodeSystem"}
           {:resourceType "ValueSet"}
           {:code "15960061000119102"
            :display
            "Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)"}
           {:code "15960141000119102"
            :display
            "Angina co-occurrent and due to coronary arteriosclerosis (disorder)"}
           {:code "15960341000119104"
            :display
            "Unstable angina due to arteriosclerosis of coronary artery bypass graft of transplanted heart (disorder)"}
           {:code "15960381000119109"
            :display
            "Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"}
           {:code "15960461000119105"
            :display
            "Unstable angina due to arteriosclerosis of autologous arterial coronary artery bypass graft (disorder)"}
           {:code "15960541000119107"
            :display
            "Unstable angina due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"}
           {:code "15960581000119102"
            :display
            "Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"}
           {:code "15960661000119107"
            :display
            "Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"}
           {:code "16754391000119100"
            :display "Stable angina due to coronary arteriosclerosis (disorder)"}
           {:code "19057007" :display "Status anginosus (disorder)"}
           {:code "194823009" :display "Acute coronary insufficiency (disorder)"}
           {:code "194828000" :display "Angina (disorder)"}
           {:code "21470009" :display "Syncope anginosa (disorder)"}
           {:code "233819005" :display "Stable angina (disorder)"}
           {:code "233821000" :display "New onset angina (disorder)"}
           {:code "300995000" :display "Exercise-induced angina (disorder)"}
           {:code "314116003" :display "Post infarct angina (disorder)"}
           {:code "315025001" :display "Refractory angina (disorder)"}
           {:code "35928006" :display "Nocturnal angina (disorder)"}
           {:code "371806006" :display "Progressive angina (disorder)"}
           {:code "371807002" :display "Atypical angina (disorder)"}
           {:code "371808007"
            :display
            "Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)"}
           {:code "371809004"
            :display
            "Recurrent angina following placement of coronary artery stent (disorder)"}
           {:code "371810009"
            :display
            "Recurrent angina status post coronary artery bypass graft (disorder)"}
           {:code "371811008"
            :display "Recurrent angina status post rotational atherectomy (disorder)"}
           {:code "371812001"
            :display
            "Recurrent angina status post directional coronary atherectomy (disorder)"}
           {:code "41334000" :display "Angina, class II (disorder)"}
           {:code "429559004" :display "Typical angina (disorder)"}
           {:code "4557003" :display "Preinfarction syndrome (disorder)"}
           {:code "59021001" :display "Angina decubitus (disorder)"}
           {:code "61490001" :display "Angina, class I (disorder)"}
           {:code "791000119109"
            :display "Angina due to type 2 diabetes mellitus (disorder)"}
           {:code "85284003" :display "Angina, class III (disorder)"}
           {:code "87343002" :display "Prinzmetal angina (disorder)"}
           {:code "89323001" :display "Angina, class IV (disorder)"}
           nil?]))))



  )
