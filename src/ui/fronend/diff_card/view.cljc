(ns ui.fronend.diff-card.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.diff-card.model]

            #?(:cljs [ui.monaco.core])))


(defn diff-card []
  (let [maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])
        maximized? (rf/subscribe [:ui.fronend.diff-card.model/maximize-card?])]
    (fn []
      (let [{:keys [card-name]} @maximized-card]
        [:div {:class [(c [:mt 5] [:bg :white] [:rounded 10] [:p 5] :overflow-hidden)
                       (when-not (= card-name :diff-card)
                           (c [:ml 10]))]}
         [:div {:class (c :flex :justify-between :items-center [:mb 2])}
          [:h3 {:class (c :text-xl :font-bold)} "Diff"]
          (if (= card-name :diff-card)
            [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                             [:hover :cursor-pointer {:filter "contrast(100%)"}])
                   :src "/static/images/arrow-left.svg"
                   :alt "back button"
                   :on-click (fn [_]
                               (do
                                 (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                               nil
                                               {}])
                                 (rf/dispatch [:ui.fronend.diff-card.model/minimize-card])))}]
            [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                             [:hover :cursor-pointer {:filter "contrast(100%)"}])
                   :src "/static/images/maximize.svg"
                   :alt "json button"
                   :on-click (fn [_]
                               (do
                                 (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                               :diff-card
                                               {}])

                                 (rf/dispatch [:ui.fronend.diff-card.model/maximize-card])))}])]
         (if @maximized?
           #?(:cljs [ui.monaco.core/monaco {:class (c [:h "500px"] [:w "500px"])}])
           [:div "Diff Stats"])]))))
