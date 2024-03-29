(ns ftr.utils.unifn.core
  (:require [clojure.spec.alpha :as s]
            [clojure.stacktrace :as stacktrace]))

(defn deep-merge [a b]
  (loop [[[k v :as i] & ks] b
         acc a]
    (if (nil? i)
      acc
      (let [av (get a k)]
        (if (= v av)
          (recur ks acc)
          (recur ks (if (and (map? v) (map? av))
                      (assoc acc k (deep-merge av v))
                      (assoc acc k v))))))))

(defmulti *fn (fn [{f ::fn}] (if (fn? f) ::function f)))

(defmethod *fn ::identitY
  [_arg] {})

(defmethod *fn ::function
  [{f ::fn :as arg}]
  (f arg))

(defmethod *fn :default
  [arg]
  {::status :error
   ::message (str "Could not resolve " (::fn arg))})

(declare *apply)

(defn apply-tracers [ctx f-name tracers phase]
  (reduce (fn [acc t]
            (deep-merge acc
                        (dissoc
                          (*apply t
                                  (deep-merge (dissoc acc ::tracers)
                                              {::tracer
                                               {:trace-ev
                                                {:f-name f-name
                                                 :phase phase}}}))
                          ::fn)))
          ctx tracers))


(defmacro evaluation-time
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (assoc ret# :ftr.utils.unifn.core/eval-time-in-ms (/ (double (- (. System (nanoTime)) start#)) 1000000.0))))


(defmacro maybe?-capture-out [expr opts]
  `(if (get ~opts ::capture-out?)
     (let [s# (new java.io.StringWriter)
           ret# (binding [*out* s#] ~expr)]
       (assoc ret# ::out (str s#)))
     ~expr))


(defn *apply-impl [{st ::status inter ::intercept f-name ::fn tracers ::tracers :as arg}]
  (if (and (not (= inter :all))
           (contains? #{:error :stop} st))
    arg
    (let [arg (dissoc arg ::intercept)
          arg  (if tracers
                 (apply-tracers arg f-name tracers :enter)
                 arg)]
      (if-let [problems (and (s/get-spec f-name) (s/explain-data f-name arg))]
        (let [ev {::status :error
                   ::fn  f-name
                   ::message (with-out-str (s/explain f-name arg))
                   ::problems (::s/problems problems)}]
           (when tracers
             (doseq [t tracers] (*apply t {:event ev :arg arg})))
           (merge arg ev))
        (let [patch
              (maybe?-capture-out
                (evaluation-time
                  (if (::safe? arg)
                    (try (*fn arg)
                         (catch Exception e
                           {::status :error
                            ::message (with-out-str (stacktrace/print-stack-trace e))}))
                    (*fn arg)))
                arg)
               patch (cond (map? patch) patch (nil? patch) {} :else {::value patch})
               patch (if-let [fx (:fx patch)]
                       (reduce (fn [res [k v]]
                                 (let [fx-res (*apply (or (get-in arg [:fx/registry k]) k) v)]
                                   (if (::status fx-res)
                                     (deep-merge patch fx-res)
                                     (when-let [pth (get v :fx/result)]
                                       (assoc-in res pth fx-res)))
                                   )) (dissoc patch :fx) fx)
                       patch)
               res ((if (:non-deep-merge (meta patch))
                      merge
                      deep-merge)
                    arg patch)]
           (if tracers
             (apply-tracers res f-name tracers :leave)
             res))))))

(defn *apply [f arg]
  (cond
    (keyword? f) (*apply-impl (assoc arg ::fn f))
    (fn? f)      (*apply-impl (assoc arg ::fn f))
    (map? f)     (*apply-impl (deep-merge arg f))
    (vector? f) (loop [[f & fs] f, arg arg]
                  (cond
                    (nil? f) arg
                    :else (recur fs (*apply f arg))))
    (var? f) (*apply (var-get f) arg)
    :else (throw (Exception. (str "I don't know how to apply " (pr-str f))))))
