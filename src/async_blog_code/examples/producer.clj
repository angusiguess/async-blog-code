(ns async-blog-code.examples.producer
  (:require [async-blog-code.kafka-client :as client]
            [clojure.core.async :as a]))

(defn publish-click
  ;; The zero-arity version returns our initial state, sort of like a
  ;; reducing function. This is so our go-loop doesn't need to know how
  ;; to initialize state. We'll use this more in the future.
  ([]
   0)
  ([connection offset publish>]
   ;; Catch any errors thrown in this function.
   (a/go (try (let [click (client/take! connection offset)]
                (a/>! publish> click)
                ;; We tag any of our return values with a message type.
                ;; For something as simple as this producer it isn't immediately useful
                ;; But this will come in handy for more complex patterns.
                [:read (inc offset)])
              (catch Throwable e
                [:error {:exception e
                         :offset offset
                         :operation :publish-click}])))))


(defn start
  ([publish> offset]
   (let [connection (client/simple-consumer 10000)]
     (a/go-loop [offset offset]
       ;; We use the message tag to determine if an error occurred. If it did, we return
       ;; the message state.
       (let [[msg-type state :as msg] (a/<! (publish-click connection offset publish>))]
         (if (= msg-type :error)
           state
           (recur state))))))
  ([publish>]
   (start publish> 0)))

(defn start-supervisor []
  (let [publish> (a/chan 1024)]
    (a/go-loop [producer (start publish>)]
      (println "Starting producer.")
      (let [{:keys [offset] :as error} (a/<! producer)]
        (println "An error has occurred: " error)
        (println "Restarting producer.")
        (recur (start publish> offset))))
    publish>))
