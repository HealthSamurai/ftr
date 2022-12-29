(ns ui.fronend.core
  (:require [re-frame.core :as rf]
            #?(:cljs [reagent.dom])
            [stylo.core :refer [c]]
            [ui.fronend.pages]
            [ui.fronend.routes]
            [clojure.string :as str]

            [ui.fronend.init-wizard.view]

            ;;zframes stuff
            #?(:cljs [ui.zframes.routing])
            [ui.zframes.rpc]))


(def page :ui.fronend.init-wizard.model/index)


(rf/reg-event-fx
  ::vs-list-vs-click
  (fn [{:keys [db]} [_ vs]]
    {:db (-> db
             (update :ui.fronend.concepts-grid.model/index dissoc :paging)
             (assoc-in [page :vs-list :selected-vs] vs)
             (dissoc :ui.fronend.diff-card.model/index  :ui.fronend.resource-card.model/index)
             (update page dissoc :selected-card))
     :zen/rpc {:method :comp
               :params {:methods [:vs-expand :vs-tag-hashes]
                        :params {:module  (get-in db [page :selected-module])
                                 :tag     (get-in db [page :selected-tag])
                                 :vs-name (:value vs)}}
               :path [page :vs-expand]}
     :ui.fronend.init-wizard.model/reset-input "conc-search"}))


(rf/reg-sub ::vs-list-selected-vs
            (fn [db _]
              (get-in db [page :vs-list :selected-vs])))


(rf/reg-event-fx
  ::search-in-vs-list
  (fn [{:keys [db]} [_ search-str]]
    {:zen/rpc {:method (if (str/blank? search-str)
                         :vs-list
                         :search-in-vs-list)
               :params {:module     (get-in db [page :selected-module])
                        :tag        (get-in db [page :selected-tag])
                        :search-str search-str}
               :path [:ui.fronend.init-wizard.model/index :value-sets]}}))


(defn value-sets-list []
  (let [value-sets (rf/subscribe [:ui.fronend.init-wizard.model/value-sets-with-display])
        selected-vs (rf/subscribe [::vs-list-selected-vs])]
    (fn []
      [:<>
       (doall
         (for [vs @value-sets]
           ^{:key (:value vs)}
           [:div {:class [(c [:px 8] [:py 2] :font-light
                             #_{:transition "0.05s"}
                             [:hover :cursor-pointer
                              {:background-color "rgb(235, 236, 241, 0.4)"} ])
                          (when (= (:value @selected-vs) (:value vs))
                            (c {:background-color "rgb(235, 236, 241) !important"}))]
                  :on-click (fn [_e] (println "AAAAAAAAAAAAAAA BLYAD") (rf/dispatch [::vs-list-vs-click vs]))}
            (:display vs)]))])))


(defn vs-list-sidebar []
  (let [selected-tag-sub (rf/subscribe [:ui.fronend.init-wizard.model/selected-tag])]
    (fn []
      [:div {:class (c :overflow-y-scroll
                       [:bg :white] :shadow-lg [:w "400px"]
                       :flex :flex-col)
             :style {:animation "example 0.06s linear"
                     :display (if @selected-tag-sub
                                "block"
                                "none")}}

       [:div {:class (c [:p 10] :border-b [:h 20] :flex :items-center)}
        [:input#vs-search
         {:class       (c  [:h 10] [:px 5] [:w 80])
          :placeholder "Search"
          :on-change (fn [e] (rf/dispatch [::search-in-vs-list (.. e -target -value)]))}]
        [:img {:class (c [:ml 2])
               :src   "/static/images/search.svg"
               :alt   "search icon"}]]
       [value-sets-list]])))


(defn layout [content]
  [:<>
   [:style "body {margin: 0}"]
   [:div {:class (c :flex :h-screen)}
    [vs-list-sidebar]
    [:div {:class (c :overflow-y-scroll
                     {:background-color "rgb(235, 236, 241)"
                      :flex "1"})}
     content]]])


(defn current-page []
  (let [{page :match params :params :as obj} @(rf/subscribe [:route-map/current-route])
        route-error @(rf/subscribe [:route-map/error])
        params (assoc params
                      :route page
                      :route-ns (when page (namespace page)))
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
                   {:db              (assoc db
                                            ::page "some data"
                                            :route-map/routes ui.fronend.routes/routes)
                    :fx [[:route-map/redirect #?(:cljs (ui.fronend.routes/href "init"))]
                         [:route-map/start {}]]}))


(defn mount-root []
  #?(:clj  #()
     :cljs (reagent.dom/render
             [current-page]
             (.getElementById js/document "root"))))


(defn init! []
  (rf/dispatch [::initialize])
  (mount-root))
