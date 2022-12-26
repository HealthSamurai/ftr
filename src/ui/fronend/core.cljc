(ns ui.fronend.core
  (:require [re-frame.core :as rf]
            #?(:cljs [reagent.dom])
            [stylo.core :refer [c]]))


(defn current-page []
  [:div {:class (c [:text :red-600] [:opacity 30])} "Hello, World!"])


(rf/reg-event-fx ::initialize
                 (fn [{db :db} _]
                   {:db (assoc db ::page "some data")}))


(defn mount-root []
  #?(:clj  #()
     :cljs (reagent.dom/render
             current-page
             (.getElementById js/document "root"))))


(defn init! []
  (rf/dispatch [::initialize])
  (mount-root))
