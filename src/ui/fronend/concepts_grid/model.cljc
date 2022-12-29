(ns ui.fronend.concepts-grid.model
  (:require [re-frame.core :as rf]
            [clojure.string :as str]))


(def page ::index)

(def wiz-page :ui.fronend.init-wizard.model/index)

(rf/reg-event-fx
  ::search-in-hash-expand
  (fn [{:keys [db]} [_ search-str]]
    {:zen/rpc
     {:method :comp
      :params {:methods [(if (str/blank? search-str)
                           :vs-expand-hash
                           :search-in-hash-expand)
                         :vs-tag-hashes]
               :params {:module       (get-in db [wiz-page :vs-expand :data :module])
                        :tag          (get-in db [wiz-page :vs-expand :data :tag])
                        :hash         (get-in db [wiz-page :vs-expand :data :hash])
                        :vs-name      (get-in db [wiz-page :vs-expand :data :vs-name])
                        :value-sets   (get-in db [wiz-page :vs-expand :data :value-sets])
                        :code-systems (get-in db [wiz-page :vs-expand :data :code-systems])
                        :search-str search-str}}
      :path [wiz-page :vs-expand]}}))


(rf/reg-sub ::concepts
            (fn [db _]
              (get-in db [wiz-page :vs-expand :data :result :concepts])))


(def page-size 20)


(rf/reg-event-fx ::toggle-paging
                 (fn [{:keys [db]} [_ size]]
                   {:db (-> db
                            (assoc-in [page :paging :page-number] 0)
                            (update-in [page :paging :disabled] (fnil not false)))}))


(rf/reg-event-fx ::set-page-size
                 (fn [{:keys [db]} [_ size]]
                   {:db (-> db
                            (assoc-in [page :paging :page-number] 0)
                            (assoc-in [page :paging :page-size] size))}))


(rf/reg-event-fx ::nth-page
                 (fn [{:keys [db]} [_ n]]
                   {:db (assoc-in db [page :paging :page-number] n)}))


(rf/reg-sub ::paging-settings
            (fn [db _]
              {:page-size   (get-in db [page :paging :page-size] page-size)
               :disabled    (get-in db [page :paging :disabled] false)
               :page-number (get-in db [page :paging :page-number] 0)}))


(defn max-page-n [els-count page-size]
  (int (dec (Math/ceil (/ els-count page-size)))))


(rf/reg-sub ::paging
            (fn [_]
              [(rf/subscribe [::paging-settings])
               (rf/subscribe [:ui.fronend.init-wizard.model/selected-vs-expand])])
            (fn [[{:keys [disabled page-size page-number]} {:keys [concepts-count]}] _]
              (let [max-page-number (max-page-n concepts-count page-size)
                    next-page-number (min max-page-number (inc page-number))
                    prev-page-number (max 0 (dec page-number))]
                {:page-size        page-size
                 :page-number      page-number
                 :disabled         disabled
                 :max-page-number  max-page-number
                 :prev-page-number (when (not= prev-page-number page-number)
                                     prev-page-number)
                 :next-page-number (when (not= next-page-number page-number)
                                     next-page-number)
                 :drop-count       (* page-size page-number)})))


(rf/reg-sub ::paginated-concepts
            (fn [_] [(rf/subscribe [:ui.fronend.init-wizard.model/selected-vs-expand])
                     (rf/subscribe [::paging])])
            (fn [[vs-expand-resp paging] _]
              (let [concepts       (get-in vs-expand-resp [:concepts])
                    concepts-count (get-in vs-expand-resp [:concepts-count])]
                (if (or (:disabled paging)
                        (<= concepts-count (+ page-size 5)))
                  {:concepts-count concepts-count
                   :concepts concepts
                   :paging {:disabled (:disabled paging)}}
                  {:paginated      true
                   :paging         paging
                   :concepts-count concepts-count
                   :concepts       (take (:page-size paging)
                                         (drop (:drop-count paging) concepts))}))))
