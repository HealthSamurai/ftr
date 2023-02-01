(ns ftr.extraction.icd10
  (:require [ftr.utils.unifn.core :as u]
            [clojure.xml]
            [clojure.zip :as zip]
            [clojure.string :as str]
            [clojure.java.io :as io]))


(defmethod u/*fn ::create-value-set [cfg]
  {::result {:value-set
             (-> (:value-set cfg)
                 (assoc :resourceType "ValueSet")
                 (->> (merge {:status  "unknown"
                              :compose {:include [{:system (get-in cfg [:code-system :url])}]}})))}})


(defmethod u/*fn ::create-code-system [cfg]
  {::result {:code-system (-> (:code-system cfg)
                              (assoc :resourceType "CodeSystem")
                              (->> (merge {:status   "unknown"
                                           :content  "not-present"
                                           :valueSet (get-in cfg [:value-set :url])})
                                   (conj #{})))}})


(defn iter-zip [zipper]
  (->> zipper
       (iterate zip/next)
       (take-while (complement zip/end?))))

(defn iter-left [zipper]
  (->> zipper
       (iterate zip/left)
       (take-while (complement nil?))))


(defn iter-right [zipper]
  (->> zipper
       (iterate zip/right)
       (take-while (complement nil?))))


(defn is-tag? [loc tag]
  (-> loc zip/node :tag #{tag}))


(defn has-child-tag? [loc tag]
  (->> loc
       (zip/down)
       (iter-right)
       (some #(is-tag? % tag))))


(defn get-code [node]
  (get-in node [:content 0 :content 0]))


(defn get-display [node]
  (get-in node [:content 1 :content 0]))


(defn assoc-chapters [xml-zipper index]
  (->> (iter-zip xml-zipper)
       (filter (comp #{:chapter} :tag zip/node))
       (reduce (fn [index loc]
                 (let [node (zip/node loc)
                       code (get-code node)
                       display (get-display node)]
                   (assoc index code {:code code
                                      :display display
                                      :hierarchy []})))
               index)))


(defn build-section-range [section-loc]
  (let [section-node (zip/node section-loc)
        section-id (get-in section-node [:attrs :id])
        section-index-loc (->> section-loc (iter-left)
                               (filter (comp #{:sectionIndex} :tag zip/node))
                               (first))
        section-ref-node   (->> section-index-loc (zip/down) (iter-right)
                                (filter (comp #{section-id} :id :attrs zip/node))
                                (first)
                                (zip/node))]
    (format "%s-%s"
            (get-in section-ref-node [:attrs :first])
            (get-in section-ref-node [:attrs :last]))))


(defn get-section-desc [section-node]
  (-> (->> section-node :content
           (filter (comp #{:desc} :tag))
           (first))
      (get-in [:content 0])))


(defn assoc-sections [xml-zipper index]
  (->> (iter-zip xml-zipper)
       (filter (every-pred (comp #{:section} :tag zip/node)
                           #(has-child-tag? % :diag)))
       (reduce (fn [index loc]
                 (let [node (zip/node loc)
                       code (build-section-range loc)
                       display (get-section-desc node)
                       parent-node (-> loc zip/up zip/node)
                       parent-chapter (get-code parent-node)]
                   (assoc index code {:code code
                                      :display display
                                      :hierarchy [parent-chapter]}))) index)))


(defn dotify-diag-code [code]
  (if (> (count code) 3)
    (str (subs code 0 3) \. (subs code 3))
    code))


(defn assoc-sections-direct-children [xml-zipper index]
  (->> (iter-zip xml-zipper)
       (filter (every-pred (comp #{:section} :tag zip/node)
                           #(has-child-tag? % :diag)))
       (mapcat (fn [loc] (let [parent-section-code (build-section-range loc)
                               child-diags (->> loc zip/down iter-right
                                                (filter (comp #{:diag} :tag zip/node))
                                                (map #(-> % zip/node (assoc-in [:attrs :parent-section-code] parent-section-code))))]
                           child-diags)))
       (reduce (fn [index node]
                 (let [code (get-code node)
                       display (get-display node)
                       parent-section-code (get-in node [:attrs :parent-section-code])
                       parent-section-hierarchy (get-in index [parent-section-code :hierarchy])
                       code (dotify-diag-code code)]
                   (assoc index code {:code code
                                      :display display
                                      :hierarchy (conj parent-section-hierarchy parent-section-code)}))) index)))


(defn parse-icd10-ordered-file
  "
  ICD-10-Ordered-File filename pattern, icd10cm-order-<YEAR_OF_RELEASE>.txt
  ICD-10-Ordered-File - sequence of lines, delimited by \n
  Each line follows this schema:
  +-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------+
  | Position  | Length  | Contents                                                                                                                                      |
  +-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------+
  | 1         | 5       | Order number, right justified, zero filled.                                                                                                   |
  | 6         | 1       | Blank                                                                                                                                         |
  | 7         | 7       | ICD-10-CM code. Dots are not included                                                                                                         |
  | 14        | 1       | Blank                                                                                                                                         |
  | 15        | 1       | 0 if the code is a “header” –not valid for HIPAA-covered transactions. 1 if the code is valid for submission for HIPAA-covered transactions.  |
  | 16        | 1       | Blank                                                                                                                                         |
  | 17        | 60      | Short description                                                                                                                             |
  | 77        | 1       | Blank                                                                                                                                         |
  | 78        | To end  | Long Description                                                                                                                              |
  +-----------+---------+-----------------------------------------------------------------------------------------------------------------------------------------------+
  "
  [path]
  (let [r (io/reader path)]
    {:parsed-icd-10 (->> r
                         (line-seq)
                         (map (fn [line]
                                {:code         (str/trim (subs line 6 13))
                                 :display      (str/trim (subs line 16 76))
                                 :definition (str/trim (subs line 77))})))
     :reader r}))


(defn set-parent-stack [curr-parent-stack {:as _current-line, :keys [code]}]
  (if
      (nil? curr-parent-stack)
    [code]
    (loop [stack curr-parent-stack]
      (if (>= (count (last stack))
              (count code))
        (recur (pop stack))
        (conj stack code)))))


(defn assoc-remaining-codes [parsed-icd-10 index]
  (loop [[{:as current-line, :keys [code]} & rest-lines] parsed-icd-10
         index index
         parent-stack nil]
    (if current-line
      (let [parent-stack (set-parent-stack parent-stack current-line)
            index        (if (get index (dotify-diag-code code))
                           index
                           (let [parent-code (dotify-diag-code (last (pop parent-stack)))
                                 code (dotify-diag-code code)
                                 hierarchy (-> index
                                               (get-in [parent-code :hierarchy])
                                               (conj parent-code))]
                             (assoc index code
                                    (assoc current-line
                                           :hierarchy hierarchy
                                           :code code))))]
        (recur rest-lines index parent-stack))
      index)))


(defmethod u/*fn ::infer-source-files-paths [{:as _cfg, :keys [source-url]}] ;;TODO Separate from extractor logic
  (let [files (file-seq (io/file source-url))]
    {::source-files-names
     {:codes (first (filter (fn [^java.io.File f]
                              (let [filename (.getName f)]
                                (and (not (str/includes? filename "addenda"))
                                     (re-find #"icd10cm-order-.+\.txt" filename))))
                            files))
      :tabular-index (first (filter (fn [^java.io.File f]
                                      (re-find #"icd10cm-tabular-.+\.xml" (.getName f)))
                                    files))}}))


(defmethod u/*fn ::import [{:as _cfg,
                            {:keys [codes tabular-index]}   ::source-files-names
                            {:keys [value-set code-system]} ::result}]

  (let [code-system (first code-system)
        parsed-xml                     (clojure.xml/parse tabular-index)
        xml-zip                        (zip/xml-zip parsed-xml)
        assoc-chapters+                 (partial assoc-chapters xml-zip)
        assoc-sections+                 (partial assoc-sections xml-zip)
        assoc-sections-direct-children+ (partial assoc-sections-direct-children xml-zip)
        {:keys [parsed-icd-10 ^java.io.BufferedReader reader]}
        (parse-icd10-ordered-file codes)
        assoc-remaining-codes+          (partial assoc-remaining-codes parsed-icd-10)
        index                          (-> {}
                                           (assoc-chapters+)
                                           (assoc-sections+)
                                           (assoc-sections-direct-children+)
                                           (assoc-remaining-codes+))]
    (.close reader)
    {::result
     {:concepts
      (->> index
           (vals)
           (map (fn [concept]
                  (assoc concept
                         :valueset [(:url value-set)]
                         :system (:url code-system)))))}}))


(defn import-from-cfg [cfg]
  (::result (u/*apply [::create-value-set
                       ::create-code-system
                       ::infer-source-files-paths
                       ::import]
                      cfg)))
