(ns ui.fronend.diff-card.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def wizard-page :ui.fronend.init-wizard.model/index)


(def page ::index )


(rf/reg-event-fx ::maximize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] true)}))


(rf/reg-event-fx ::minimize-card
                 (fn [{db :db} & _]
                   {:db (assoc-in db [page :maximized?] false)}))


(rf/reg-sub ::maximize-card?
            (fn [db _]
              (get-in db [page :maximized?])))


(rf/reg-event-fx ::select-hash-succ
                 (fn [{db :db} [_ {data :data type :type}]]
                   {:db (assoc-in db [page :selected-hash type] (:tf-content data))}))


(rf/reg-event-fx ::select-hash
                 (fn [{db :db} [_ type val]]
                   {:zen/rpc {:method :tf-content
                              :params {:module (get-in db [wizard-page :selected-module])
                                       :vs-name (get-in db [wizard-page :vs-expand :data :vs-name])
                                       :hash val}
                              :success {:event ::select-hash-succ
                                        :type type}}}))


(rf/reg-sub ::selected-hashes
            (fn [db _]
              (get-in db [page :selected-hash])))
