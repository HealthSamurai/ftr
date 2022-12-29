(ns ui.fronend.init-wizard.view
  (:require [ui.fronend.pages]
            [ui.fronend.init-wizard.model :as model]
            [stylo.core :refer [c]]
            [re-frame.core :as rf]

            [ui.fronend.concepts-grid.view]
            [ui.fronend.resource-card.view]
            [ui.fronend.version-stepper.view]))


(defn init-wizard []
  (let [modules (rf/subscribe [:ui.fronend.init-wizard.model/module-list])
        tags (rf/subscribe [:ui.fronend.init-wizard.model/tags-list])
        wizard-breadcrumb (rf/subscribe [:ui.fronend.init-wizard.model/wizard-breadcrumb])
        selected-module (rf/subscribe [:ui.fronend.init-wizard.model/selected-module])
        selected-tag (rf/subscribe [:ui.fronend.init-wizard.model/selected-tag])
        selected-vs (rf/subscribe [:ui.fronend.core/vs-list-selected-vs])]
    (fn []
      [:div {:class (c [:px 10] [:py 5] {:background-color "rgb(235, 236, 241)"})}
       [:h1 {:class (c :font-bold :text-3xl)}
        [:div {:class (c :flex :items-baseline)}
         [:img {:class (c [:mr 2] :inline [:h "20px"]
                          [:hover :cursor-pointer])
                :src "/static/images/nav-arrow-left.svg"
                :alt "back button"
                :on-click (fn [_] (rf/dispatch [:ui.fronend.init-wizard.model/back-via-breadcrumb]))}]
         @wizard-breadcrumb]]
       (when @selected-vs
         [ui.fronend.version-stepper.view/tag-stepper])
       [:div {:class (c :flex)}
        (cond
          (not @selected-module)
          [:div {:class (c [:pt 5] [:pl "28px"])}
           (for [module @modules]
             ^{:key module} [:<>
                             [:span {:class (c :text-xl [:mb 1]
                                               :font-light
                                               {:transition "0.05s"}
                                               [:hover :cursor-pointer
                                                :font-bold])
                                     :on-click (fn [_] (rf/dispatch [:ui.fronend.init-wizard.model/select-module module]))}
                              module]
                             [:br]])]

          (not @selected-tag)
          [:div {:class (c [:pt 5] [:pl "28px"])}
           (for [tag @tags]
             ^{:key tag} [:<>
                          [:span {:class (c :text-xl [:mb 1]
                                            :font-light
                                            {:transition "0.05s"}
                                            [:hover :cursor-pointer
                                             :font-bold])
                                  :on-click (fn [_] (rf/dispatch [:ui.fronend.init-wizard.model/select-tag tag]))} tag]
                          [:br]])]


          (some? @selected-vs)
          [:<>
           [ui.fronend.concepts-grid.view/concept-grid]
           [ui.fronend.resource-card.view/resource-column]])]])))


(ui.fronend.pages/reg-page model/page init-wizard)
