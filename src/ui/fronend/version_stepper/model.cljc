(ns ui.fronend.version-stepper.model
  (:require [re-frame.core :as rf]))


(def wiz-page :ui.fronend.init-wizard.model/index)


(rf/reg-event-fx ::switch-version
  (fn [{db :db} [_ hash]]
    {:zen/rpc {:method :comp
               :params {:methods [:vs-expand-hash :vs-tag-hashes]
                        :params {:module  (get-in db [wiz-page :selected-module])
                                 :tag     (get-in db [wiz-page :selected-tag])
                                 :vs-name (get-in db [wiz-page :vs-expand :data :vs-name])
                                 :hash hash}}
               :path [wiz-page :vs-expand]}}))
