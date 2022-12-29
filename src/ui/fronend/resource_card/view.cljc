(ns ui.fronend.resource-card.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.resource-card.model :as model]
            [clojure.reflect :as r]))


(defn simplified-resource-view [resource]
  [:div
   (for [[k v] resource]
     [:div {:class (c :truncate)}
      [:span {:class (c :font-semibold :uppercase)} (str (name k) ":")]
      [:span {:class (c [:ml 2] :font-light)} v]])])


(defn resource-card [resource]
  (let [maximized-card (rf/subscribe [:ui.fronend.init-wizard.model/maximized-card])]
    (fn [resource]
      (let [{:keys [card-name]} @maximized-card]
        [:div {:class (c [:w-max 100] [:mb 10] [:bg :white] [:rounded 10] [:p 5])}
         [:div {:class (c :flex :justify-between :items-center [:mb 2])}
          [:h3 {:class (c :text-xl :font-bold)} (:resourceType resource)]
          (if (= card-name :resource-card)
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
                                                   :resource-card
                                                   {:resource resource}]))}])]
         (simplified-resource-view
           (select-keys resource [:url :name :id :active :status :content]))]))))


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
           (for [resource (concat @vss @css)]
             [resource-card resource])])))))
