(ns test
  (:import com.abahgat.suffixtree.GeneralizedSuffixTree
           com.abahgat.suffixtree.Utils)
  (:require [clojure.tools.build.api :as b]
            [cheshire.core]
            [clojure.java.io :as io]))


(defn deaccent [str]
  "Remove accent from string"
  ;; http://www.matt-reid.co.uk/blog_post.php?id=69
  (let [normalized (java.text.Normalizer/normalize str java.text.Normalizer$Form/NFD)]
    (clojure.string/replace normalized #"\p{InCombiningDiacriticalMarks}+" "")))


(defn suffix-tree []
  {:index []
   :tree (com.abahgat.suffixtree.GeneralizedSuffixTree.)})


(defn put! [tree k v]
  (let [i         (count (:index tree))
        new-index (conj (:index tree) v)]
    (prn "putting " k)
    (time (.put (:tree tree)
                (-> k
                    deaccent
                    com.abahgat.suffixtree.Utils/normalize)
                i))
    {:index new-index
     :tree (:tree tree)}))


(defn search [tree s]
  (map (:index tree)
       (.search (:tree tree) s)))


(defn parse-ndjson-gz [path]
  (with-open [rdr (-> path
                      (io/input-stream)
                      (java.util.zip.GZIPInputStream.)
                      (io/reader))]
    (->> rdr
         line-seq
         (mapv (fn [json-row]
                 (cheshire.core/parse-string json-row keyword))))))


(comment
  (def snomed
    (time
      (parse-ndjson-gz "https://storage.googleapis.com/ftr/snomed/vs/http%3A--snomed.info-sct/tf.ee1857e1ac0d06fe4118956f86dff3e58a86e67b3034e82b610a326f95efc10a.ndjson.gz")))

  (def snomed-concepts
    (drop 2 snomed))


  (def snomed-displays
    (map :display snomed-concepts))

  (sort-by key (frequencies (map #(* 50 (quot (count %) 50)) snomed-displays)))

  (def snomed-display-tree
    (time (reduce (fn [acc concept] (put! acc (:display concept) concept))
                  (suffix-tree)
                  snomed-concepts)))

  (let [s (io/writer "/tmp/out.txt")]
    (binding [*out* s]
      (def snomed-trees
        (time (doall (map #(reduce (fn [acc concept] (put! acc (:display concept) concept))
                                   (suffix-tree)
                                   %)
                          snomed-partitions))))
      :done))


  (def snomed-partitions
    (time (partition-all 50000 snomed-concepts)))

  (def snomed-trees
    (time (doall (map #(reduce (fn [acc concept] (put! acc (:display concept) concept))
                               (suffix-tree)
                               %)
                      snomed-partitions))))

  (defn find-in-trees [trees s]
    (apply concat
           (map #(search % s)
                trees)))
  (+ 1 1)

  (time
    (count (map :display (find-in-trees snomed-trees "weapon"))))

  )


(comment
  (b/javac {:src-dirs ["/Users/kgofhedgehogs/.gitlibs/libs/abahgat/suffixtree/b95b632e7a88ab8175714e0c4a55488048fb2d04/src/main/java"]
            :basis (b/create-basis {:project "deps.edn"})
            :class-dir "target/classes"})

  )
