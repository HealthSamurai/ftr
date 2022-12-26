(ns ui.fronend.dev
  (:require [ui.fronend.core :as core]
            [devtools.core]))


(devtools.core/install!)

(defn ^:dev/after-load re-render []
  (core/mount-root))

(core/init!)
