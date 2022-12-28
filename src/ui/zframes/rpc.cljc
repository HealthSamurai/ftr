(ns ui.zframes.rpc
  (:require [re-frame.core :as rf]
            [re-frame.db :as db]))

(defn to-json [x]
  #?(:cljs (js/JSON.stringify (clj->js x))))

(defn from-json [x]
  #?(:cljs (js->clj (js/JSON.parse x) :keywordize-keys true)))

(defonce debounce-state (atom {}))

(defn *rpc-fetch [{:keys [path debounce force success error silent-alerts transport] :as opts}]
  #?(:cljs
     (if (and debounce path (not force))
       (do
         (when-let [t (get @debounce-state path)]
           (js/clearTimeout t))
         (swap! debounce-state assoc path (js/setTimeout #(*rpc-fetch (assoc opts :force true)) debounce)))
       (let [db db/app-db
             dispatch-event (fn [event payload]
                              (when (:event event)
                                (rf/dispatch [(:event event) (merge event {:request opts} payload)])))]

         (when path
           (swap! db assoc-in (conj path :loading) true)
           (swap! db assoc-in (conj path :status) :loading))

         (-> (js/fetch (str (aget js/window "api-base-url") "/rpc?_format=" (or (some-> transport name) "json ") "&_m=" (:method opts))
                       (clj->js {:method "post", :mode "cors",
                                 :headers {"accept" "application/json", "Cache-Control" "no-cache",
                                           "Content-Type" (case (some-> transport name)
                                                            "json" "application/json"
                                                            "application/json" )}
                                 :cache "no-store",
                                 :body (let [payload (select-keys opts [:method :params :id :policy])]
                                         (case (some-> transport name)
                                           "json" (to-json payload)
                                           (to-json payload)))}
                                :keyword-fn (fn [x] (subs (str x) 1))))
             (.then
              (fn [resp]
                (.then (.text resp)
                       (fn [doc]
                         (let [cdoc (case (some-> transport name)
                                      "json"    (from-json doc)
                                      (from-json doc))]
                           (if-let [res  (and (< (.-status resp) 299) (or (:result cdoc) (get cdoc "result")))]
                             (do (swap! db update-in path merge {:status :ok :loading false :data res})
                                 (when success (dispatch-event success {:response resp :data res})))
                             (do
                               (swap! db update-in path merge {:status :error :loading false :error (or (:error cdoc) cdoc)})
                               (when error
                                 (dispatch-event error {:response resp :data (or (:error cdoc) cdoc)})))))))))
             (.catch (fn [err]
                       (.error js/console err)
                       (swap! db update-in path merge {:status :error :loading false :error {:err err}})
                       (when error
                         (dispatch-event error {:error err})))))))))

(defn rpc-fetch [opts]
  (when opts
    (if (vector? opts)
      (doseq [o opts] (when (some? o) (*rpc-fetch o)))
      (*rpc-fetch opts))))

(rf/reg-fx :zen/rpc rpc-fetch)
