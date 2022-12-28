(ns ui.fronend.init-wizard.view
  (:require [ui.fronend.pages]
            [ui.fronend.init-wizard.model :as model]
            [stylo.core :refer [c]]))


(defn init-wizard []
  [:div {:class (c {:background-color "rgb(235, 236, 241)"} [:rounded 45])}
   "I'm init wizard!"])


(ui.fronend.pages/reg-page model/page init-wizard)
