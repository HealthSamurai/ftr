(ns ui.fronend.concepts-grid.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.concepts-grid.model :as model]))



(def table-cell (c {:border-collapse "collapse"} :truncate :text-sm [:py 2] [:px 5] [:w-min 35] [:w-max 35]))
(def extended-table-cell (c {:border-collapse "collapse"} :truncate :text-sm [:py 2] [:px 5] [:w-min 100] [:w-max 100]))

(defn concept-grid  []
  (let [paginated-concepts-sub (rf/subscribe [::model/paginated-concepts])
        maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])]
    (fn []
      (let [{:keys [card-name]} @maximized-card]
        (cond
          (or (= card-name :concepts-grid)
              (nil? card-name))
          (let [{:as paginated-concepts
                 :keys [concepts concepts-count]} @paginated-concepts-sub]
            [:div {:class (c [:mt 5])}
             [:div {:class (c [:bg :white] [:rounded 10] [:p 5] :overflow-hidden)}
              [:div {:class (c :flex :justify-between :items-center [:mb 5])}
               [:div {:class (c :flex :items-center [:space-x 4])}
                [:h2 {:class (c :text-xl :font-bold)} "Concepts: " concepts-count]
                [:div {:class (c :flex :items-center)}
                 [:input#conc-search
                  {:class       (c  [:h 10] [:px 5] :border [:rounded 10])
                   :placeholder "Search"
                   :on-change (fn [e] (rf/dispatch [::model/search-in-hash-expand
                                                    (.. e -target -value)]))}]]]
               (if (= card-name :concepts-grid)
                 [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                                  [:hover :cursor-pointer {:filter "contrast(100%)"}])
                        :src "/static/images/arrow-left.svg"
                        :alt "minimize button"
                        :on-click (fn [_] (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                                        nil
                                                        {}]))}]
                 [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                                  [:hover :cursor-pointer {:filter "contrast(100%)"}])
                        :src "/static/images/maximize.svg"
                        :alt "maximize button"
                        :on-click (fn [_] (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                                        :concepts-grid
                                                        {}]))}])]

              [:span {:on-click (fn [_] (rf/dispatch [::model/set-page-size 10]))}
               (if (= 10 (get-in paginated-concepts [:paging :page-size]))
                 "[10]"
                 "10")]
              [:span {:on-click (fn [_] (rf/dispatch [::model/set-page-size 20]))}
               (if (= 20 (get-in paginated-concepts [:paging :page-size]))
                 "[20]"
                 "20")]
              [:span {:on-click (fn [_] (rf/dispatch [::model/set-page-size 50]))}
               (if (= 50 (get-in paginated-concepts [:paging :page-size]))
                 "[50]"
                 "50")]
              [:span {:on-click (fn [_] (rf/dispatch [::model/set-page-size concepts-count]))}
               (if (= concepts-count (get-in paginated-concepts [:paging :page-size]))
                 (str "[" concepts-count "]")
                 (str concepts-count))]
              [:br]
              (when-let [prev-page-number (get-in paginated-concepts [:paging :prev-page-number])]
                [:span {:on-click (fn [_] (rf/dispatch [::model/nth-page prev-page-number]))}
                 "[prev-page]" prev-page-number])
              (when-let [next-page-number (get-in paginated-concepts [:paging :next-page-number])]
                [:span {:on-click (fn [_] (rf/dispatch [::model/nth-page next-page-number]))}
                 "[next-page]" next-page-number])

              [:div {:class (c :flex :flex-col)}
               [:div {:class (c :flex {:position "sticky" :top 0})}
                [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c [:bg :white] :font-bold [:border-b :black])]} "Code"]
                [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c [:bg :white] :font-bold :border-l [:border-b :black])]} "Display"]
                [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c [:bg :white] :font-bold :border-l [:border-b :black])]} "System"]]
               (for [concept concepts]
                 ^{:key (or (:id concept) (str (:system concept) (:code concept)))}
                 [:div {:class (c :flex :font-light)}
                  [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c [:border-b :black])]} (:code concept)]
                  [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c :border-l [:border-b :black])]} (:display concept)]
                  [:div {:class [(if (= card-name :concepts-grid) extended-table-cell table-cell) (c :border-l [:border-b :black])]} (:system concept)]])]]])

          card-name
          nil)))))
