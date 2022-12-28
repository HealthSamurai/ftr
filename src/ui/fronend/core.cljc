(ns ui.fronend.core
  (:require [re-frame.core :as rf]
            #?(:cljs [reagent.dom])
            [stylo.core :refer [c]]
            [ui.fronend.pages]
            [ui.fronend.routes]

            [ui.fronend.init-wizard.view]

            ;;zframes stuff
            #?(:cljs [ui.zframes.routing])
            [ui.zframes.rpc]))


(defn layout [content]
  [:<>
   [:style "body {margin: 0}"]
   [:div {:class (c :flex :h-screen)}
    [:div {:class (c [:bg :white] :shadow-lg [:w "25%"])}]
    [:div {:class (c {:background-color "rgb(235, 236, 241)"} [:w "75%"])}
     content]]])


(defn current-page []
  (let [{page :match params :params :as obj} @(rf/subscribe [:route-map/current-route])
        route-error @(rf/subscribe [:route-map/error])
        params (assoc params
                      :route page
                      :route-ns (when page (namespace page)))
        _ (println "PAGE?" params)
        content (if page
                  (if-let [cmp (get @ui.fronend.pages/pages page)]
                    [cmp params]

                    [:div.not-found (str "Page not found [" (str page) "]")])
                  (case route-error
                    nil [:div]
                    :not-found [:div.not-found (str "Route not found ")]
                    [:div.not-found (str "Routing error")]))]
    [layout content]))


(rf/reg-event-fx ::initialize
                 (fn [{db :db} _]
                   {:db              (assoc db ::page "some data")
                    :route-map/start {}}))


(defn mount-root []
  #?(:clj  #()
     :cljs (reagent.dom/render
             [current-page]
             (.getElementById js/document "root"))))


(defn init! []
  (rf/dispatch [::initialize])
  (mount-root))
