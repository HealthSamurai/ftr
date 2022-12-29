(ns ui.fronend.diff-card.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.diff-card.model]

            #?(:cljs [ui.monaco.core])))


(defn tag-selector [type]
  (let [hashes-list (rf/subscribe [:ui.fronend.init-wizard.model/hashes-list])]
    (fn [type]
      [:select {:on-change (fn [e] (rf/dispatch [:ui.fronend.diff-card.model/select-hash type (.. e -target -value)]))}
       [:option "Select Hash"]
       (doall
         (for [tag @hashes-list]
           [:option {:value tag} (str (subs tag 0 6) "...")]))])))


(defn diff-card []
  (let [maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])
        maximized? (rf/subscribe [:ui.fronend.diff-card.model/maximize-card?])
        selected-hashes (rf/subscribe [:ui.fronend.diff-card.model/selected-hashes])]
    (fn []
      (let [{:keys [card-name]} @maximized-card
            selected-hashes @selected-hashes]
        (when (or (nil? card-name) (= card-name :diff-card))
            [:div {:class [(c [:mt 5] [:bg :white] [:rounded 10] [:p 5] :overflow-hidden)
                        (when-not (= card-name :diff-card)
                          (c [:ml 10]))]}
          [:div {:class (c :flex :justify-between :items-center [:space-x 50])}
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
          (when @maximized?
            [:div
             [:div
              [tag-selector :from]
              [:span {:class (c :text-xs)} "<>"]
              [tag-selector :to]]

             (when (and
                     (get-in selected-hashes [:from])
                     (get-in selected-hashes [:to]))
               #?(:cljs [ui.monaco.core/monaco {:class (c [:h "1000px"] [:w "1000px"])
                                                :from (get-in selected-hashes [:from])
                                                :to (get-in selected-hashes [:to])}]))])])))))
