(ns ui.fronend.version-stepper.view
  (:require [stylo.core :refer [c]]
            [re-frame.core :as rf]
            [ui.fronend.version-stepper.model]))


(defn tag-stepper []
  (let [hashes (rf/subscribe [:ui.fronend.init-wizard.model/hashes-list])
        current-hash (rf/subscribe [:ui.fronend.init-wizard.model/selected-hash])]
    (fn []
      [:div {:class (c :flex :items-center [:my 5])}
       (doall
         (for [h @hashes]
           [:<>
            ^{:key h}
            [:div {:on-click (fn [_] (rf/dispatch [:ui.fronend.version-stepper.model/switch-version h]))
                   :class [(c [:rounded :full] [:w 3] [:h 3] [:border 1 :black]
                              {:transition "0.07s"}
                              [:hover [:bg :black] :cursor-pointer])
                           (when (= h @current-hash)
                             (c [:bg :black]))]}
             ^{:key (str h "stick")}
             [:div {:class (c :relative :text-xs
                              [:bottom -3])}
              (str (subs h 0 5) "...")]]
            [:div {:class (c [:last-child :hidden] [:w 25] [:h "1px"] [:bg :black])}]]))])))
