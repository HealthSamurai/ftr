(ns ftr.logger.core
  (:require [ftr.utils.unifn.core :as u]
            [progrock.core :as pr]
            [clojure.string :as str]))


(defmulti dispatch-logger (fn [{:as _ctx,
                                {{:keys [f-name phase]} :trace-ev} :ftr.utils.unifn.core/tracer}]
                            [f-name phase]))


(defmethod dispatch-logger [:ftr.core/feeder :enter]
  [{:as _ctx,
    :keys [extraction-result]}]
  (println "\n📦 Commit")
  {::statistics (atom {:patches 0
                       :tfs 0
                       :pb (pr/progress-bar (count extraction-result))})})


(defmethod dispatch-logger [:ftr.core/write-terminology-file :leave]
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


(defmethod dispatch-logger [:ftr.patch-generator.core/generate-patch :leave]
  [{:as _ctx,
    ::keys [statistics]
    {patch-created? :patch-created?} :patch-generation-result}]
  (when (and statistics patch-created?)
    (swap! statistics update :patches inc)
    nil))


(defmethod dispatch-logger [:ftr.core/feeder :leave]
  [{:as _ctx,
    ::keys [statistics]
    {:keys [ftr-path]} :cfg}]
  (pr/print (pr/done (-> statistics deref :pb)) {:format ":progress/:total   :percent% [:bar] Processing ValueSet: "})
  (println (str \newline "Results: "))
  (println (format "    Terminology Files created: \033[0;1m%s\033[22m" (-> statistics deref :tfs)))
  (println (format "    Patches created: \033[0;1m%s\033[22m" (-> statistics deref :patches)))
  (println (format "    FTR Repository size: \033[0;1m%s MB\033[22m" (int (/ (ftr.utils.core/psize ftr-path) 1000000)))))


(defmethod dispatch-logger :default [{:as _ctx,
                                      {{:keys [f-name phase]} :trace-ev} :ftr.utils.unifn.core/tracer}]
  (println (format "%s \033[0;1m%s\033[22m" (str/capitalize (name phase)) (str/capitalize (name f-name)))))


(defmethod u/*fn ::dispatch-logger [ctx]
  (dispatch-logger ctx))
