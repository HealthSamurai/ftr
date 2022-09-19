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
                       :expected-tf-sha256           "70c1225a2ddd108c869a18a87a405c029f48a30c0401268869f19959a4723976"
                       :expected-tf-filename         "tf.70c1225a2ddd108c869a18a87a405c029f48a30c0401268869f19959a4723976.ndjson.gz"
                       :expected-updated-tf-sha256   "476b3dbcdab7fe4e8522451f7495c185317acb3530178b31c91a888e173f94f5"
                       :expected-updated-tf-filename "tf.476b3dbcdab7fe4e8522451f7495c185317acb3530178b31c91a888e173f94f5.ndjson.gz"
                       :expected-tag-tf-sha256       "a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b"
                       :expected-tag-tf-filename     "tf.a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b.ndjson.gz"
                       :expected-patch-filename      "patch.70c1225a2ddd108c869a18a87a405c029f48a30c0401268869f19959a4723976.476b3dbcdab7fe4e8522451f7495c185317acb3530178b31c91a888e173f94f5.ndjson.gz"
                       :expected-patch2-filename     "patch.476b3dbcdab7fe4e8522451f7495c185317acb3530178b31c91a888e173f94f5.a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b.ndjson.gz"})


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
          [{:name (format "%s.%s" module value-set-name) :hash "a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b"}
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
            [{:tag new-tag :hash "a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b" :from-tag old-tag}
             {:from (:expected-updated-tf-sha256 csv-test-env-cfg) :to "a68ffc7e6b868ea62d2696563cbd1c57e42c4adc2ebf90324da4208c443aff3b"}
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
     :expected-tf1-sha256         "9cd86290024e7ce323fefd54a9462ce350df41aafde6a5fac25cdab36cb3f5d1"
     :expected-tf1-filename       "tf.9cd86290024e7ce323fefd54a9462ce350df41aafde6a5fac25cdab36cb3f5d1.ndjson.gz"
     :expected-updated-tf1-sha256 "7d6388be98678cb546ea0b74ba1e296fad911478c591a0769cda2841bffa27a3"
     :expected-updated-tf1-filename "tf.7d6388be98678cb546ea0b74ba1e296fad911478c591a0769cda2841bffa27a3.ndjson.gz"
     :expected-tf1-patch-filename "patch.9cd86290024e7ce323fefd54a9462ce350df41aafde6a5fac25cdab36cb3f5d1.7d6388be98678cb546ea0b74ba1e296fad911478c591a0769cda2841bffa27a3.ndjson.gz"
     :expected-tf2-sha256   "a40321eca14464aaf3e5e9e0df0b158dd1cd0d72b1f54e52872266fb345fa5bb"
     :expected-tf2-filename "tf.a40321eca14464aaf3e5e9e0df0b158dd1cd0d72b1f54e52872266fb345fa5bb.ndjson.gz"
     :expected-updated-tf2-sha256 "ef7fe0a16510d9dd342dd521d54953327f15aa5e4f5e0fc2a5f9dd9111584a1b"
     :expected-updated-tf2-filename "tf.ef7fe0a16510d9dd342dd521d54953327f15aa5e4f5e0fc2a5f9dd9111584a1b.ndjson.gz"
     :expected-tf2-patch-filename "patch.a40321eca14464aaf3e5e9e0df0b158dd1cd0d72b1f54e52872266fb345fa5bb.ef7fe0a16510d9dd342dd521d54953327f15aa5e4f5e0fc2a5f9dd9111584a1b.ndjson.gz"
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
                                                (:tag user-cfg)))})]

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
          "http:--hl7.org-fhir-administrative-gender-female"
          "http:--hl7.org-fhir-administrative-gender-male"
          "http:--hl7.org-fhir-administrative-gender-other"
          "http:--hl7.org-fhir-administrative-gender-unknown"
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
