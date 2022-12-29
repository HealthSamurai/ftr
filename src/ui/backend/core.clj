(ns ui.backend.core
  (:require [org.httpkit.server :as http-kit]
            [zen.core]
            [route-map.core]
            [ui.backend.routes]
            [ftr.utils.unifn.core :as u]
            [ring.util.response]
            [ring.middleware.head]
            [ring.util.codec :as codec]
            [clojure.string :as str]
            [cheshire.core :as cheshire]
            [clojure.java.io :as io]
            [clojure.reflect :as r]))


(defn handler [{:as                          ctx,
                {:keys [request-method uri]} :request
                {:keys [routes]}             :config}]
  (let [{:as   _resolved-route
         :keys [match]}
        (route-map.core/match [request-method uri] routes)
        response (:response (u/*apply match ctx))]
    response))


(defn set-content-type [response uri]
  (cond-> response
    (str/ends-with? uri ".svg")
    (assoc-in [:headers "Content-Type"] "image/svg+xml")))


(defn handle-static [h {meth :request-method uri :uri :as req}]
  (if (and (contains? #{:get :head} meth)
           (str/starts-with? (or uri "") "/static/"))

    (let [opts {:root "public"
                :index-files? true
                :allow-symlinks? true}
          path (subs (codec/url-decode (:uri req)) 8)]
      (-> (ring.util.response/resource-response path opts)
          (ring.middleware.head/head-response req)
          (set-content-type uri)))
    (h req)))


(defn wrap-static [h]
  (fn [req]
    (handle-static h req)))


(defn handle-body [h {body :body :as req}]
  (cond-> req
    body (update :body (fn [body]
                         (cond
                           (= (get-in req [:headers "content-type"]) "application/json")
                           (cheshire/parse-stream (io/reader body)  keyword)

                           :else body)))
    :always (h)))


(defn wrap-body [h]
  (fn [req]
    (handle-body h req)))


(defn handle-response [h req]
  (let [response (h req)]
    (update response :body (fn [body]
                             (cond-> body (= (get-in response [:headers "Content-Type"]) "application/json")
                               (cheshire/generate-string))))))


(defn wrap-response [h]
  (fn [req]
    (handle-response h req)))


(defn start [config]
  (let [ztx (zen.core/new-context)
        ctx (atom {:config config
                   :zen ztx
                   :suffix-trees-cache (atom {})})
        _ (zen.core/read-ns ztx 'facade)
        handler-wrapper (-> (fn [req & [opts]]
                              (handler (assoc @ctx
                                              :request req)))
                            (wrap-static)
                            (wrap-body)
                            (wrap-response))
        server-stop-fn (http-kit/run-server handler-wrapper (:web config))]
    (swap! ctx assoc :handler-wrapper handler-wrapper
           :server-stop-fn server-stop-fn)))


(comment
  (def server (start {:web {:port 7777}
                      :routes ui.backend.routes/routes}))

  ((:server-stop-fn server ))

  )
