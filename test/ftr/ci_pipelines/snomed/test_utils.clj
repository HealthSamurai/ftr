(ns ftr.ci-pipelines.snomed.test-utils
  (:require  [clojure.test :as t]
             [org.httpkit.server]
             [org.httpkit.server :as http-kit]
             [hiccup.page]))


(def mock-snomed-archive-url "/snomed-archive.zip")


(defn generate-snomed-version-info-html-with-download-url
  []
  (hiccup.page/html5
   [:body
    [:h4
     [:a#anch_8 {:href "#download-the-us-edition-of-snomed-ct"
                 :name "download-the-us-edition-of-snomed-ct"}
      "Download the US Edition of SNOMED CT"]]
    [:p
     "https://uts-ws.nlm.nih.gov/download"
     [:br]
     [:b "?url="]
     mock-snomed-archive-url
     [:br]
     [:b "&apiKey="]
     "YOUR_API_KEY "]]))


(defn mock-handler [req]
  (case (:uri req)
    "/snomed-version-info.html"
    {:status 200
     :body (generate-snomed-version-info-html-with-download-url)
     :headers {"Content-Type" "text/html; charset=utf-8"}}))

(def mock-server-opts {:port 7654})

(defn start-mock-server [] (http-kit/run-server #'mock-handler
                                                mock-server-opts))

(def mock-server-url (format "http://localhost:%s" (:port mock-server-opts)))


(comment
  (def mock-server (start-mock-server))

  (mock-server)

  nil)
