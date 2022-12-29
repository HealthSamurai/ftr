(ns ui.fronend.concepts-grid.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def page ::index)

(def wiz-page :ui.fronend.init-wizard.model/index)

(rf/reg-event-fx ::search-in-hash-expand
                 (fn [{:keys [db]} [_ search-str]]
                   {:zen/rpc
                    {:method :comp
                     :params {:methods [:search-in-hash-expand :vs-tag-hashes]
                              :params {:module       (get-in db [wiz-page :vs-expand :data :module])
                                       :tag          (get-in db [wiz-page :vs-expand :data :tag])
                                       :hash         (get-in db [wiz-page :vs-expand :data :hash])
                                       :vs-name      (get-in db [wiz-page :vs-expand :data :vs-name])
                                       :value-sets   (get-in db [wiz-page :vs-expand :data :value-sets])
                                       :code-systems (get-in db [wiz-page :vs-expand :data :code-systems])
                                       :search-str search-str}}
                     :path [wiz-page :vs-expand]}}))
