(ns ui.fronend.pages)


(defonce pages (atom {}))


(defn reg-page
  "register page under keyword for routing"
  [key f & [_layout-key]]
  (swap! pages assoc key f))
