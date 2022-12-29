(ns ui.fronend.resource-card.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.resource-card.model :as model]
            [clojure.pprint]))


(defn simplified-resource-view [resource]
  [:div
   (for [[k v] resource]
     [:div {:class (c :truncate)}
      [:span {:class (c :font-semibold :uppercase)} (str (name k) ":")]
      [:span {:class (c [:ml 2] :font-light)} v]])])


(defn resource-card [resource]
  (let [maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])
        maximized? (rf/subscribe [:ui.fronend.resource-card.model/maximize-card?])]
    (fn [resource]
      (let [{:keys [card-name]} @maximized-card]
        [:div {:class [(c [:w 100] [:mb 10] [:bg :white] [:rounded 10] [:p 5])
                       (when (= card-name :resource-card)
                         (c :w-screen))]}
         [:div {:class (c :flex :justify-between :items-center [:mb 2])}
          [:h3 {:class (c :text-xl :font-bold)} (:resourceType resource)]
          (if (= card-name :resource-card)
            [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                             [:hover :cursor-pointer {:filter "contrast(100%)"}])
                   :src "/static/images/arrow-left.svg"
                   :alt "back button"
                   :on-click (fn [_]
                               (do
                                 (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                               nil
                                               {}])
                                 (rf/dispatch [:ui.fronend.resource-card.model/minimize-card])))}]
            [:img {:class (c :inline [:h "20px"] {:filter "contrast(1%)"}
                             [:hover :cursor-pointer {:filter "contrast(100%)"}])
                   :src "/static/images/code-brackets-square.svg"
                   :alt "json button"
                   :on-click (fn [_]
                               (do
                                 (rf/dispatch [:ui.fronend.init-wizard.model/maximize-card
                                               :resource-card
                                               {:resource resource}])

                                 (rf/dispatch [:ui.fronend.resource-card.model/maximize-card])))}])]
         (if @maximized?
           [:div {:class (c [:p 5] {:white-space "pre"}
                            :overflow-x-scroll :overflow-y-scroll)}
            (with-out-str (clojure.pprint/pprint resource))]
           (simplified-resource-view
             (select-keys resource [:url :name :id :active :status :content])))]))))


(defn resource-column []
  (let [vss (rf/subscribe [::model/selected-vss])
        css (rf/subscribe [::model/selected-css])
        maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])]
    (fn []
      (let [{:keys [card-name resource]} @maximized-card]
        (cond
          (= card-name :resource-card)
          [:div {:class (c [:mt 5])}
           [resource-card resource]]

          card-name
          nil

          :else
          [:div {:class (c [:pt 5] [:ml 10] :flex :flex-col)}
           (doall
             (for [resource (concat @vss @css)]
              [resource-card resource]))])))))
