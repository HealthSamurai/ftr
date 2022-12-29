(ns ui.monaco.core
  (:require [reagent.core :as r]
            [reagent.dom :as dom]
            [re-frame.core :as rf]))


(def ^js/Object monacoe (when-let [m (aget js/window "monaco")]
                          (aget m  "editor")))


(defn monaco [props]
  (let [editor (atom nil)]
    (r/create-class
     {:reagent-render (fn [props attrs] [:div.monaco {:class (:class props)}])

      :component-did-mount
      (fn [this]
        (let [^js/Object e (.createDiffEditor ^js/Object monacoe (dom/dom-node this)
                                              #js{:language "clojure"
                                                  :theme "vs"
                                                  :minimap #js{:enabled false}
                                                  :renderSideBySide true
                                                  :enableSplitViewResizing false
                                                  :glyphMargin false
                                                  :folding false
                                                  :lineDecorationsWidth 10
                                                  :lineNumbersMinChars 3
                                                  :lineNumbers "on"
                                                  :scrollbar #js{:horizontal "hidden"
                                                                 :vertical "hidden"}})]

          (.setModel e #js{:original (.createModel monacoe (:from props) "json")
                           :modified (.createModel monacoe (:to props) "json")})

          (reset! editor e)))

      :component-will-unmount
      (fn [this]
        (when-let [^js/Object e @editor]
          (.dispose e)))


      :component-did-update
      (fn [this _]
        (.setModel ^js/Object @editor
                   #js{:original (.createModel monacoe (.getValue (goog.object/get (.getModel ^js/Object @editor) "modified")) "clojure")
                       :modified (.createModel monacoe (:value (r/props this)) "clojure")}))})))

(defn index []
  (let [value (r/atom "key: val\n")]
    (fn []
      [:div "Monaco"
       [:style ".monaco {width: 500px; min-height: 200px;}"]
       [:button {:on-click #(reset! value "key2: val2\n")} "Change value"]
       [:pre (pr-str @value)]
       [monaco {:value @value}]])))
