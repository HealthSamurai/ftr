(ns ui.fronend.resource-card.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.resource-card.model :as model]))


(defn simplified-resource-view [resource]
  [:div
   (for [[k v] resource]
     [:div {:class (c :truncate)}
      [:span {:class (c :font-semibold :uppercase)} (str (name k) ":")]
      [:span {:class (c [:ml 2] :font-light)} v]])])


(defn resource-card [resource]
  [:div {:class (c [:w-max 100] [:mb 10] [:bg :white] [:rounded 10] [:p 5])}
   [:h3 {:class (c :text-xl :font-bold [:mb 2])} (:resourceType resource)]
   (simplified-resource-view
     (select-keys resource [:url :name :id :active :status :content]))])


(defn resource-column []
  (let [vss (rf/subscribe [::model/selected-vss])
        css (rf/subscribe [::model/selected-css])]
    (fn []
      [:div {:class (c [:pt 5] [:ml 10] :flex :flex-col)}
       (for [resource (concat @vss @css)]
         [resource-card resource])])))
