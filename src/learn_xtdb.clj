(ns learn-xtdb
  (:require [xtdb.api :as xt]
            [learn-xtdb.docs :as docs]))

;;;;;;;; Learn XTDB Datalog Today
;; https://nextjournal.com/try/learn-xtdb-datalog-today/learn-xtdb-datalog-today


;; start an in-memory instance
(def my-node (xt/start-node {}))
;; => #'learn-xtdb/my-node

;; load the data
(xt/submit-tx my-node
              (for [doc docs/my-docs]
                [:xtdb.api/put doc]))
;; => #:xtdb.api{:tx-id 0, :tx-time #inst "2023-03-20T09:15:24.933-00:00"}

(xt/sync my-node)
;; => #inst "2023-03-20T09:15:24.933-00:00"

;; check loaded data
(xt/q (xt/db my-node)
      '{:find [title]
        :where [[_ :movie/title title]]})
;; => #{["First Blood"] ["Terminator 2: Judgment Day"] ["The Terminator"] ["Rambo III"] ["Predator 2"] ["Lethal Weapon"] ["Lethal Weapon 2"] ["Lethal Weapon 3"] ["Alien"] ["Aliens"] ["Die Hard"] ["Rambo: First Blood Part II"] ["Commando"] ["Mad Max 2"] ["Mad Max"] ["RoboCop"] ["Braveheart"] ["Mad Max Beyond Thunderdome"] ["Predator"] ["Terminator 3: Rise of the Machines"]}

;; abstract query fn
(defn q [query & args]
  (apply xt/q (xt/db my-node) query args))

;; check again using new query fn
(q '{:find [title]
     :where [[_ :movie/title title]]})
;; => #{["First Blood"] ["Terminator 2: Judgment Day"] ["The Terminator"] ["Rambo III"] ["Predator 2"] ["Lethal Weapon"] ["Lethal Weapon 2"] ["Lethal Weapon 3"] ["Alien"] ["Aliens"] ["Die Hard"] ["Rambo: First Blood Part II"] ["Commando"] ["Mad Max 2"] ["Mad Max"] ["RoboCop"] ["Braveheart"] ["Mad Max Beyond Thunderdome"] ["Predator"] ["Terminator 3: Rise of the Machines"]}
