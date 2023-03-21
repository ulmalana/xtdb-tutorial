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

;;; basis queries
(q '{:find [e]
     :where [[e :person/name "Ridley Scott"]]})

(q '{:find [e]
     :where [[e :person/name]]})

;; exercise 1: entity id of movies made in 1987
(q '{:find [e]
     :where [[e :movie/year 1987]]})
;; => #{[-202] [-203] [-204]}

;; exercise 2: find eid and titles of movie
(q '{:find [e title]
     :where [[e :movie/title title]]})
;; => #{[-210 "Rambo III"] [-203 "Lethal Weapon"] [-216 "Mad Max"] [-207 "Terminator 2: Judgment Day"] [-211 "Predator 2"] [-200 "The Terminator"] [-208 "Terminator 3: Rise of the Machines"] [-218 "Mad Max Beyond Thunderdome"] [-213 "Lethal Weapon 3"] [-219 "Braveheart"] [-214 "Alien"] [-205 "Commando"] [-217 "Mad Max 2"] [-215 "Aliens"] [-206 "Die Hard"] [-212 "Lethal Weapon 2"] [-201 "First Blood"] [-204 "RoboCop"] [-202 "Predator"] [-209 "Rambo: First Blood Part II"]}

;; exercise 3: find name of all people
(q '{:find [person-name]
     :where [[_ :person/name person-name]]})
;; => #{["Rae Dawn Chong"] ["Joe Pesci"] ["Brian Dennehy"] ["Nick Stahl"] ["Carrie Henn"] ["Tom Skerritt"] ["George P. Cosmatos"] ["Paul Verhoeven"] ["Alan Rickman"] ["Peter MacDonald"] ["Alexander Godunov"] ["Bruce Willis"] ["Tina Turner"] ["Claire Danes"] ["Danny Glover"] ["Mark L. Lester"] ["Ridley Scott"] ["Peter Weller"] ["Bruce Spence"] ["Michael Preston"] ["Jonathan Mostow"] ["Ruben Blades"] ["Sigourney Weaver"] ["Joanne Samuel"] ["Stephen Hopkins"] ["Michael Biehn"] ["George Ogilvie"] ["Ted Kotcheff"] ["Steve Bisley"] ["Charles Napier"] ["Carl Weathers"] ["Robert Patrick"] ["John McTiernan"] ["Richard Donner"] ["Marc de Jonge"] ["Gary Busey"] ["Sylvester Stallone"] ["Nancy Allen"] ["Mel Gibson"] ["Elpidia Carrillo"] ["Ronny Cox"] ["Veronica Cartwright"] ["Edward Furlong"] ["Richard Crenna"] ["Arnold Schwarzenegger"] ["James Cameron"] ["Alyssa Milano"] ["Sophie Marceau"] ["George Miller"] ["Linda Hamilton"]}

;;; data patterns
(q '{:find [title]
     :where [[e :movie/year 1987]
             [e :movie/title title]]})
;; => #{["Lethal Weapon"] ["RoboCop"] ["Predator"]}

(q '{:find [name]
     :where [[m :movie/title "Lethal Weapon"]
             [m :movie/cast p]
             [p :person/name name]]})
;; => #{["Danny Glover"] ["Gary Busey"] ["Mel Gibson"]}

;; exercise 1: find movie titles made in 1985
(q '{:find [title]
     :where [[m :movie/year 1985]
             [m :movie/title title]]})
;; => #{["Rambo: First Blood Part II"] ["Commando"] ["Mad Max Beyond Thunderdome"]}

;; exercise 2: when was "Alien" released?
(q '{:find [year]
     :where [[m :movie/title "Alien"]
             [m :movie/year year]]})
;; => #{[1979]}

;; exercise 3: who directed "RoboCop"?
(q '{:find [name]
     :where [[m :movie/title "RoboCop"]
             [m :movie/director p]
             [p :person/name name]]})
;; => #{["Paul Verhoeven"]}

;; exercise 4: who directed Arnold Schwarzenegger in a movie
(q '{:find [name]
     :where [[p :person/name "Arnold Schwarzenegger"]
             [m :movie/cast p]
             [m :movie/director d]
             [d :person/name name]]})
;; => #{["Mark L. Lester"] ["Jonathan Mostow"] ["John McTiernan"] ["James Cameron"]}

;;; parameterized queries
(q '{:find [title]
     :in [name] ;; parameter
     :where [[p :person/name name]
             [m :movie/cast p]
             [m :movie/title title]]}
   "Sylvester Stallone") ;; argument
;; => #{["First Blood"] ["Rambo III"] ["Rambo: First Blood Part II"]}

;; using tuple as param
(q '{:find [title]
     :in [[director actor]]
     :where [[d :person/name director]
             [a :person/name actor]
             [m :movie/director d]
             [m :movie/cast a]
             [m :movie/title title]]}
   ["James Cameron" "Arnold Schwarzenegger"])
;; => #{["Terminator 2: Judgment Day"] ["The Terminator"]}

;; or

(q '{:find [title]
      :in [director actor]
      :where [[d :person/name director]
              [a :person/name actor]
              [m :movie/director d]
              [m :movie/cast a]
              [m :movie/title title]]}
   "James Cameron" "Arnold Schwarzenegger")
;; => #{["Terminator 2: Judgment Day"] ["The Terminator"]}

;; using collections for query
(q '{:find [title]
     :in [[director ...]] ;; use all value in argument
     :where [[p :person/name director]
             [m :movie/director p]
             [m :movie/title title]]}
   ["James Cameron" "Ridley Scott"])
;; => #{["Terminator 2: Judgment Day"] ["The Terminator"] ["Alien"] ["Aliens"]}

;; relations using tuple
(q '{:find [title earning]
     :in [director [[title earning]]]
     :where [[p :person/name director]
             [m :movie/director p]
             [m :movie/title title]]}
   "Ridley Scott"
   [["Die Hard" 1000000]
    ["Alien" 2000000]
    ["Lethal Weapon" 300000]
    ["Commando" 400000]])
;; => #{["Alien" 2000000]}

;; exercise 1: find movie title by year
(q '{:find [title]
     :in [year]
     :where [[m :movie/year year]
             [m :movie/title title]]}
   1989)
;; => #{["Lethal Weapon 2"]}

;; exercise 2: given a list of titles, find the title and the year it was released
(q '{:find [title year]
     :in [[title ...]]
     :where [[m :movie/title title]
             [m :movie/year year]]}
   ["Alien" "Commando" "Die Hard" "Lethal Weapon"])
;; => #{["Lethal Weapon" 1987] ["Commando" 1985] ["Die Hard" 1988] ["Alien" 1979]}

;; exercise 3: find all titles where actor and director has worked together
(q '{:find [title]
     :in [actor director]
     :where [[a :person/name actor]
             [d :person/name director]
             [m :movie/director d]
             [m :movie/cast a]
             [m :movie/title title]]}
   "Michael Biehn"
   "James Cameron")
;; => #{["The Terminator"] ["Aliens"]}

;; exercise 4: given an actor name and relation with title/rating,
;; find the title and corresponding rating for which that actor was a cast
(q '{:find [title rating]
     :in [actor [[title rating]]]
     :where [[a :person/name actor]
             [m :movie/cast a]
             [m :movie/title title]]}
   "Mel Gibson"
   [["Die Hard" 8.3]
    ["Alien" 8.5]
    ["Lethal Weapon" 7.6]
    ["Commando" 6.5]
    ["Mad Max Beyond Thunderdome" 6.1]
    ["Mad Max 2" 7.6]
    ["Rambo: First Blood Part II" 6.2]
    ["Braveheart" 8.4]
    ["Terminator 2: Judgment Day" 8.6]
    ["Predator 2" 6.1]
    ["First Blood" 7.6]
    ["Aliens" 8.5]
    ["Terminator 3: Rise of the Machines" 6.4]
    ["Rambo III" 5.4]
    ["Mad Max" 7.0]
    ["The Terminator" 8.1]
    ["Lethal Weapon 2" 7.1]
    ["Predator" 7.8]
    ["Lethal Weapon 3" 6.6]
    ["RoboCop" 7.5]])
;; => #{["Mad Max" 7.0] ["Mad Max 2" 7.6] ["Lethal Weapon" 7.6] ["Lethal Weapon 3" 6.6] ["Lethal Weapon 2" 7.1] ["Braveheart" 8.4] ["Mad Max Beyond Thunderdome" 6.1]}
