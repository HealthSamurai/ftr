(ns ui.fronend.init-wizard.model
  (:require [re-frame.core :as rf]))


(def page ::index )


(rf/reg-event-fx ::index
                 (fn [{db :db} [_ phase _params]]
                   (case phase
                     :init nil
                     :params nil
                     :deinit nil
                     nil)))
