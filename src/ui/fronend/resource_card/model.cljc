(ns ui.fronend.resource-card.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def wizard-page :ui.fronend.init-wizard.model/index)


(rf/reg-sub ::selected-vss
            (fn [db _]
              (get-in db [wizard-page :vs-expand :data :value-sets])))


(rf/reg-sub ::selected-css
            (fn [db _]
              (get-in db [wizard-page :vs-expand :data :code-systems])))
