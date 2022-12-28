(ns ui.backend.routes
  (:require [ui.backend.operations]))


(def routes
  {"ui" {:GET :ui.backend.operations/ui}
   "rpc" {:POST :ui.backend.operations/rpc}})
