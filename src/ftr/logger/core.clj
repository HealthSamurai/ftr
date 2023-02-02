(ns ftr.logger.core
  (:require [ftr.utils.unifn.core :as u]
            [ftr.utils.core]
            [progrock.core :as pr]
            [clojure.string :as str]))


(defmulti dispatch-unit-logger (fn [{:as _ctx,
                                {{:keys [f-name phase]} :trace-ev} :ftr.utils.unifn.core/tracer}]
                            [f-name phase]))


(defmethod dispatch-unit-logger [:ftr.core/feeder :enter]
  [{:as _ctx,
    :keys [extraction-result]}]
  (println "\n📦 Commit")
  {::statistics (atom {:patches 0
                       :tfs 0
                       :pb (pr/progress-bar (count extraction-result))})})


(defmethod dispatch-unit-logger [:ftr.core/write-terminology-file :leave]
  [{:as _ctx,
    ::keys [statistics]
    {:keys [vs-url]} :ftr.core/feeder}]
  (when statistics
    (let [_ (swap! statistics update :tfs inc)
         pb (-> statistics deref :pb)]
     (pr/print pb
               {:format (format ":progress/:total   :percent%% [:bar] Processing ValueSet: %s" vs-url)})
     (swap! statistics assoc :pb (pr/tick pb))
     nil)))


(defmethod dispatch-unit-logger [:ftr.patch-generator.core/generate-patch :leave]
  [{:as _ctx,
    ::keys [statistics]
    {patch-created? :patch-created?} :patch-generation-result}]
  (when (and statistics patch-created?)
    (swap! statistics update :patches inc)
    nil))


(defmethod dispatch-unit-logger [:ftr.core/feeder :leave]
  [{:as _ctx,
    ::keys [statistics]
    {:keys [ftr-path]} :cfg}]
  (pr/print (pr/done (-> statistics deref :pb)) {:format ":progress/:total   :percent% [:bar] Processing ValueSet: "})
  (println (str \newline "Results: "))
  (println (format "    Terminology Files created: \033[0;1m%s\033[22m" (-> statistics deref :tfs)))
  (println (format "    Patches created: \033[0;1m%s\033[22m" (-> statistics deref :patches)))
  (println (format "    FTR Repository size: \033[0;1m%s MB\033[22m" (int (/ (ftr.utils.core/psize ftr-path) 1000000)))))


(defmethod dispatch-unit-logger :default [_])


(defmethod u/*fn ::dispatch-unit-logger [ctx]
  (dispatch-unit-logger ctx))


(defn calc-left-pad [call-stack]
  (when call-stack
    (apply str (repeat (* (count call-stack) 2) " "))))


(defonce default-out *out*)


(defmethod u/*fn ::log-step [{:as _ctx,
                              eval-time-in-ms :ftr.utils.unifn.core/eval-time-in-ms
                              {{:keys [f-name phase]} :trace-ev} :ftr.utils.unifn.core/tracer
                              :ftr.utils.unifn.core/keys [out capture-out?]
                              ::keys [call-stack]}]
  (binding [*out* default-out]
    (let [prettified-fn-name (str/capitalize (name f-name))]
      (case phase
        :enter
        (do
          (println (str (calc-left-pad call-stack)
                        (format  "\033[0;1m%s\033[22m" prettified-fn-name)))
          ^:non-deep-merge {:ftr.utils.unifn.core/capture-out? true
                            ::call-stack ((fnil conj []) call-stack f-name)})

        :leave
        (let [call-stack ((fnil pop []) call-stack)
              left-pad (calc-left-pad call-stack)]
          (when (and capture-out? (seq out))
            (println (str/join \newline (map #(str left-pad %) (str/split out #"\n")))))
          (println (str left-pad
                        (format "\u001B[32m%s\u001B[0m %s ms" prettified-fn-name eval-time-in-ms)))
          ^:non-deep-merge {::call-stack call-stack})))))
