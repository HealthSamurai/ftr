(ns ui.fronend.concepts-grid.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]))



(def table-cell (c {:border-collapse "collapse"} [:py 2] [:px 5] [:w-min 35] [:w-max 35]))


(defn concept-grid  []
  (let [selected-vs-expand (rf/subscribe [:ui.fronend.init-wizard.model/selected-vs-expand])]
    (fn []
      (let [{:keys [concepts concepts-count]} @selected-vs-expand]
        [:div {:class (c [:pl "28px"] [:mt 5])}
         [:div {:class (c [:bg :white] [:rounded 10] [:p 2] :overflow-hidden)}
          [:h2 {:class (c :text-2xl [:mb 5])} "Concepts: " concepts-count]
          [:div {:class (c :flex :flex-col)}
           [:div {:class (c :flex {:position "sticky" :top 0})}
            [:div {:class [table-cell (c [:bg :white] :font-bold [:border-b :black])]} "Code"]
            [:div {:class [table-cell (c [:bg :white] :font-bold :border-l [:border-b :black])]} "Display"]]
           (for [concept concepts]
             ^{:key (or (:id concept) (str (:system concept) (:code concept)))}
             [:div {:class (c :flex :font-light)}
              [:div {:class [table-cell (c [:border-b :black])]} (:code concept)]
              [:div {:class [table-cell (c :border-l [:border-b :black])]} (:display concept)]])]]]))))
