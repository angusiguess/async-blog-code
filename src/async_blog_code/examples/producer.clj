(ns async-blog-code.examples.producer
  (:require [async-blog-code.kafka-client :as client]
            [clojure.core.async :as a]))

;; First one, no error handling.

(defn start []
  (let [connection (client/simple-consumer 10)
        publish> (a/chan)]
    (a/go-loop [offset 0]
      (let [click (client/take! connection offset)]
        (a/>! publish> click)
        (recur (inc offset))))
    publish>))
