(ns ui.fronend.init-wizard.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def page ::index)


(rf/reg-sub ::module-list
            (fn [db _]
              (get-in db [page :modules :data :modules])))


(rf/reg-sub ::tags-list
            (fn [db _]
              (get-in db [page :tags :data :tags])))


(rf/reg-sub ::selected-module
            (fn [db _]
              (get-in db [page :selected-module])))


(rf/reg-sub ::selected-tag
            (fn [db _]
              (get-in db [page :selected-tag])))


(rf/reg-sub ::value-sets
            (fn [db _]
              (get-in db [page :value-sets :data])))


(rf/reg-sub ::wizard-breadcrumb
            (fn [_]
              [(rf/subscribe [::selected-module])
               (rf/subscribe [::selected-tag])])

            (fn [[selected-module selected-tag] _]
              (str/join " / " (filter identity [(when-not selected-module "Init Wizard")
                                                (or selected-module "Select Module:")
                                                (when selected-module (or selected-tag "Select Tag:"))]))))


(rf/reg-event-fx ::get-modules-list
                 (fn [{db :db} & _]
                   (when-not (seq (get-in db [page :modules]))
                     {:zen/rpc {:method :module-list
                                :path [page :modules]}})))


(rf/reg-event-fx ::select-module
                 (fn [{db :db} [_ module]]
                   {:db (assoc-in db [page :selected-module] module)
                    :zen/rpc {:method :module-tags
                              :params {:module module}
                              :path [page :tags]}}))


(rf/reg-event-fx ::select-tag
                 (fn [{db :db} [_ tag]]
                   {:db (assoc-in db [page :selected-tag] tag)
                    :dispatch [::load-value-sets]}))


(rf/reg-event-fx ::back-via-breadcrumb
                 (fn [{db :db} & _]
                   (let [selected-module (get-in db [page :selected-module])
                         selected-tag (get-in db [page :selected-tag])]
                     {:db (cond
                            selected-tag
                            (update db page dissoc :selected-tag)

                            selected-module
                            (update db page dissoc :selected-module)

                            :else
                            db)})))


(rf/reg-event-fx ::load-value-sets
                 (fn [{db :db} & _]
                   (let [selected-module (get-in db [page :selected-module])
                         selected-tag (get-in db [page :selected-tag])]
                     {:zen/rpc {:method :vs-list
                                :params {:module selected-module
                                         :tag selected-tag}
                                :path [page :value-sets]}})))


(rf/reg-event-fx ::index
                 (fn [{db :db} [_ phase _params]]
                   (case phase
                     :init {:dispatch [::get-modules-list]}
                     :params nil
                     :deinit nil
                     nil)))
