(ns ui.fronend.init-wizard.view
  (:require [ui.fronend.pages]
            [ui.fronend.init-wizard.model :as model]
            [stylo.core :refer [c]]))


(defn init-wizard []
  [:div {:class (c  [:px 10] [:py 5] {:background-color "rgb(235, 236, 241)"} [:rounded 45])}
   [:h1 {:class (c :font-bold :text-3xl)} "Init Wizard"]])


(ui.fronend.pages/reg-page model/page init-wizard)
