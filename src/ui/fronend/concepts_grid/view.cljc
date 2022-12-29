(ns ui.fronend.concepts-grid.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.concepts-grid.model :as model]))



(def table-cell (c {:border-collapse "collapse"} :truncate :text-sm [:py 2] [:px 5] [:w-min 35] [:w-max 35]))

(defn concept-grid  []
  (let [selected-vs-expand (rf/subscribe [:ui.fronend.init-wizard.model/selected-vs-expand])]
    (fn []
      (let [{:keys [concepts concepts-count]} @selected-vs-expand]
        [:div {:class (c [:mt 5])}
         [:div {:class (c [:bg :white] [:rounded 10] [:p 5] :overflow-hidden)}
          [:div {:class (c :flex :justify-between :items-center [:mb 5])}
           [:h2 {:class (c :text-xl :font-bold)} "Concepts: " concepts-count]
           [:div {:class (c :flex :items-center)}
            [:input#conc-search
             {:class       (c  [:h 10] [:px 5] :border [:rounded 10])
              :placeholder "Search"
              :on-change (fn [e] (rf/dispatch [::model/search-in-hash-expand
                                               (.. e -target -value)]))}]]
           [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                            [:hover :cursor-pointer {:filter "contrast(100%)"}])
                  :src "/static/images/maximize.svg"
                  :alt "maximize button"}]]
          [:div {:class (c :flex :flex-col)}
           [:div {:class (c :flex {:position "sticky" :top 0})}
            [:div {:class [table-cell (c [:bg :white] :font-bold [:border-b :black])]} "Code"]
            [:div {:class [table-cell (c [:bg :white] :font-bold :border-l [:border-b :black])]} "Display"]
            [:div {:class [table-cell (c [:bg :white] :font-bold :border-l [:border-b :black])]} "System"]]
           (for [concept concepts]
             ^{:key (or (:id concept) (str (:system concept) (:code concept)))}
             [:div {:class (c :flex :font-light)}
              [:div {:class [table-cell (c [:border-b :black])]} (:code concept)]
              [:div {:class [table-cell (c :border-l [:border-b :black])]} (:display concept)]
              [:div {:class [table-cell (c :border-l [:border-b :black])]} (:system concept)]])]]]))))
