(ns ui.backend.operations
  (:require [ftr.utils.unifn.core :as u]
            [cheshire.core]
            [hiccup.core]
            [hiccup.page]
            [stylo.core]
            [org.httpkit.server :as server]
            [clojure.walk]
            [clojure.string :as str]))


(defmethod u/*fn ::root [ctx]
  {:response {:status 200
              :body (hiccup.page/html5
                     [:head
                      [:link {:href "/static/css/stylo.css"
                              :type "text/css"
                              :rel  "stylesheet"}]
                      [:link {:href "/static/css/main.css"
                              :type "text/css"
                              :rel  "stylesheet"}]]
                     [:body
                      [:div#root]
                      [:script {:src (str "/static/js/frontend.js")}]])}})
