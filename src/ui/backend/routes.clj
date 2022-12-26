(ns ui.backend.routes
  (:require [ui.backend.operations]))


(def routes
  {:GET :ui.backend.operations/root})
