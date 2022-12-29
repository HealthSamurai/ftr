(ns ui.fronend.resource-card.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def wizard-page :ui.fronend.init-wizard.model/index)


(def page ::index)


(rf/reg-sub ::selected-vss
            (fn [db _]
              (get-in db [wizard-page :vs-expand :data :value-sets])))


(rf/reg-sub ::selected-css
            (fn [db _]
              (get-in db [wizard-page :vs-expand :data :code-systems])))


(rf/reg-event-fx ::maximize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] true)}))


(rf/reg-event-fx ::minimize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] false)}))


(rf/reg-sub ::maximize-card?
            (fn [db _]
              (get-in db [page :maximized?])))
