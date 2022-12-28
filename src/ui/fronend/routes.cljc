(ns ui.fronend.routes
  (:require [clojure.string :as str]
            [re-frame.core :as rf]
            [route-map.core :as route-map]
            [ui.zframes.window-location :as location]))


(def routes {:. :ui.fronend.init-wizard.model/index})


(defn get-url-alias
  [url]
  (->> (route-map.core/match [:. url] routes)
       (:path)
       (mapv (fn [path] (if (keyword? path) [path] path)))
       (get-in routes)
       (:alias)))


(defn to-query-params [params]
  (->> params
       (map (fn [[k v]] (str (name k) "=" v)))
       (str/join "&")))


#?(:cljs
   (defn href [& parts]
     (let [params (if (map? (last parts)) (last parts) nil)
           parts (if params (butlast parts) parts)
           url (str "/" (str/join "/" (map (fn [x] (if (keyword? x) (name x) (str x))) parts)))]
       (when-not  (route-map/match [:. url] routes)
         (println (str url " is not matches routes")))
       (str "#" url (when (seq (filter second params))
                      (location/gen-query-string (into {} (filter second params))))))))


(defn back [fallback]
  {:href (or @(rf/subscribe [:pop-route]) fallback)})
