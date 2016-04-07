(ns async-blog-code.kafka-client
  (:require [clojure.test.check.generators :as gen]
            [clj-uuid :as uuid]))

;; Let's assume click data has the following:
;; user-id, click

(def gen-click-data (gen/hash-map :user-id gen/uuid
                                  :click (gen/elements [:add-to-cart :shipping :payment])))


;; Simplifying assumptions:
;; One topic.
;; Two exceptions, one for a bad offset, one for a session timeout.
;; Data has already been processed into clojure maps.

(defn invalid-offset-exception [offset]
  (ex-info "An invalid offset has been specified!"
           {:offset offset}))

(defn kafka-exception [offset]
  (ex-info "Session has timed out."
           {:offset offset}))

(defprotocol LogConsumer
  (take! [this offset]))

;; A *very* simple model of a consumer for an append-only log. Initialized with
;; a vector of values and provides them to a client by 'offset' which in this case
;; is just the index of that value.

(defrecord SimpleConsumer
    [events]
  LogConsumer
  (take! [this offset]
    (if (= 0 (rand-int 1000)) (throw (kafka-exception offset))
        (try (nth events offset)
             (catch Exception e
               (throw (invalid-offset-exception offset)))))))

(defn simple-consumer [n]
  (let [events (into [] (gen/sample gen-click-data n))]
    (SimpleConsumer. events)))
