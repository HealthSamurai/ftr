(ns ftr.core-test
  (:require [ftr.core :as sut]
            [ftr.pull.core :as pull-sut]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [matcho.core :as matcho]
            [ftr.utils.core]))


(defn fs-tree->tree-map [path]
  (reduce
    (fn [store path] (assoc-in store path {}))
    {}
    (map (fn [f] (drop 1 (str/split (str f) #"/"))) (file-seq (io/file path)))))


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

(t/deftest generate-repository-layout-from-csv-source
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

  (t/testing "tag move"
    (let [{:as user-cfg, :keys [module ftr-path tag]
           {{value-set-name :url} :value-set} :extractor-options
           {:keys [old-tag new-tag]} :move-tag}
          (assoc csv-user-cfg
                 :source-url (:csv-source-tag csv-test-env-cfg)
                 :tag "v2"
                 :move-tag {:old-tag "v1"
                            :new-tag "v2"})

          value-set-name (ftr.utils.core/escape-url value-set-name)


          old-tf-tag-file-name
          (format "tag.%s.ndjson.gz" old-tag)

          new-tf-tag-file-name
          (format "tag.%s.ndjson.gz" new-tag)

          _
          (sut/apply-cfg {:cfg user-cfg})

          ftr-tree
          (get-in (fs-tree->tree-map ftr-path) (str/split (subs ftr-path 1) #"/"))]


      (t/testing "sees updated repository layout, new tf sha is correct, patch file created"
        (matcho/match
         ftr-tree
         {module
          {"tags"
           {(format "%s.ndjson.gz" old-tag) {}
            (format "%s.ndjson.gz" new-tag) {}}
           "vs"
           {value-set-name
            {(:expected-tf-filename csv-test-env-cfg)         {}
             (:expected-updated-tf-filename csv-test-env-cfg) {}
             (:expected-patch-filename csv-test-env-cfg)      {}
             old-tf-tag-file-name                             {}
             new-tf-tag-file-name                             {}}}}}))

      (t/testing "sees tag ingex content"
        (matcho/match
         (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/tags/%s.ndjson.gz" (:ftr-path csv-test-env-cfg) old-tag))
         [{:name (format "%s.%s" module value-set-name) :hash (:expected-updated-tf-sha256 csv-test-env-cfg)}
          nil?])

        (matcho/match
          (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/tags/%s.ndjson.gz" (:ftr-path csv-test-env-cfg) new-tag))
          [{:name (format "%s.%s" module value-set-name) :hash "4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff"}
           nil?]))

      (t/testing "sees terminology tag file"
        (t/testing (format "%s tag" old-tag)
          (matcho/match
            (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/vs/%s/%s" ftr-path value-set-name old-tf-tag-file-name))
            [{:tag old-tag :hash (:expected-updated-tf-sha256 csv-test-env-cfg)}
             {:from (:expected-tf-sha256 csv-test-env-cfg) :to (:expected-updated-tf-sha256 csv-test-env-cfg)}
             nil?]))

        (t/testing (format "%s tag" new-tag)
          (matcho/match
            (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/vs/%s/%s" ftr-path value-set-name new-tf-tag-file-name))
            [{:tag new-tag :hash "4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff" :from-tag old-tag}
             {:from (:expected-updated-tf-sha256 csv-test-env-cfg) :to "4bd40bdf6d93b9dd45e2b8f678596ddf0ce2ae5c7fb2866882211e548cd781ff"}
             nil?])))

      (t/testing "sees new patch file"
        (matcho/match
          (sort-by :code (ftr.utils.core/parse-ndjson-gz (format "%s/icd10/vs/%s/%s" ftr-path value-set-name (:expected-patch2-filename csv-test-env-cfg))))
          [{:name value-set-name}
           {:code "AA" :op "remove"}
           {:code "V01-X59" :op "remove"}
           {:code "XX" :op "remove"}
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
            (ftr.utils.core/parse-ndjson-gz (format "%s/ig-plan.ndjson.gz"
                                                    (:ftr-path user-cfg)))
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
                  :update-plan-name-path "/tmp/igftr/update-plan.ndjson.gz"
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
                           :move-tag  {:old-tag "v1"
                                       :new-tag "v2"}
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
                                (assoc :update-plan-name (:update-plan-name env) :tag (:tag user-cfg))
                                (dissoc :move-tag)))
          nil?)))

    (sut/apply-cfg {:cfg (assoc user-cfg :source-url (:ig-source-updated1 env))})
    (sut/apply-cfg {:cfg (merge user-cfg {:source-url (:ig-source-updated2 env)})})
    (sut/apply-cfg {:cfg (merge user-cfg {:source-url (:ig-source-updated3 env)
                                          :tag        "v2"
                                          :move-tag   {:old-tag "v1"
                                                       :new-tag "v2"}})})


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
               {:tag       "v2"
                :move-tag  {:old-tag "v1"
                            :new-tag "v2"}
                :tag-index (ftr.utils.core/parse-ndjson-gz
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

        _ (sut/apply-cfg {:cfg (merge user-cfg
                                      {:tag "v2"
                                       :move-tag {:old-tag "v1"
                                                  :new-tag "v2"}})})

        _ (t/testing "ftr shape correct"
            (matcho/match
              (get-in (fs-tree->tree-map ftr-path) (drop 1 (str/split ftr-path #"/")))
              {"dehydrated"
               {"vs"
                {"gender-vs"
                 {"tf.935b647b83da220fd0d351634a98a40dd53efa2e26461159e512e15e89a19e7b.ndjson.gz" {}
                  "tf.d4fe7700488a6f482fb97b12091fe1cc3b5096045aa853df07ede853c17f2530.ndjson.gz" {}
                  "tag.v1.ndjson.gz" {}
                  "patch.d4fe7700488a6f482fb97b12091fe1cc3b5096045aa853df07ede853c17f2530.935b647b83da220fd0d351634a98a40dd53efa2e26461159e512e15e89a19e7b.ndjson.gz" {}
                  "tag.v2.ndjson.gz" {}}}
                "tags" {"v2.ndjson.gz" {} "v1.ndjson.gz" {}}}}))

        update-plan-name "update-plan"
        update-plan-path (str ftr-path \/ update-plan-name ".ndjson.gz")

        _ (t/testing "pull/migrate create correct update plan, remove plan remains empty"
            (matcho/match
              (update (pull-sut/migrate (assoc client-cfg :update-plan-name update-plan-name)) :remove-plan sort)
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
