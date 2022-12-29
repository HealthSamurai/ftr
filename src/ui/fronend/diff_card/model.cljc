(ns ui.fronend.diff-card.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def wizard-page :ui.fronend.init-wizard.model/index)


(def page ::index)


(rf/reg-event-fx ::maximize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] true)}))


(rf/reg-event-fx ::minimize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] false)}))


(rf/reg-sub ::maximize-card?
            (fn [db _]
              (get-in db [page :maximized?])))
