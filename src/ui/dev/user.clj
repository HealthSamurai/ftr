(ns ui.dev.user
  (:require [shadow.cljs.devtools.api :as shadow]
            [shadow.cljs.devtools.config :as shadow.config]
            [shadow.cljs.devtools.server :as shadow.server]
            [clojure.java.io :as io]
            [ui.backend.core]
            [ui.backend.routes]))


(defn delete-recursively [f]
  (when (.isDirectory ^java.io.File f)
    (doseq [c (.listFiles ^java.io.File f)]
      (delete-recursively c)))
  (io/delete-file f))


(defn restart-ui [& [clean]]
  (let [cfg (shadow.config/load-cljs-edn!)]
    (shadow.server/stop!)
    (let [f (io/file ".shadow-cljs")]
      (when (.exists f)
        (delete-recursively f)))
    (when clean
      (doseq [[bid _] (:builds cfg)]
        (when-not (= :npm bid)
          (try (-> (shadow.config/get-build bid)
                   (get-in [:dev :output-dir])
                   (io/file)
                   (delete-recursively))
               (catch Exception _)))))
    (shadow.server/start!)
    (doseq [[bid _] (:builds cfg)]
      (println "BID>" bid)
      (when-not (= :npm bid)
        (shadow/watch bid)))))


(comment
  (restart-ui)

  (def server (ui.backend.core/start
                {:web {:port 7777}
                 :routes ui.backend.routes/routes}))

  ((:server-stop-fn server ))

  )
