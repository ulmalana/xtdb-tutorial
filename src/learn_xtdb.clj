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

;;; predicates

;; find all movies released before 1984
(q '{:find [title]
     :where [[m :movie/title title]
             [m :movie/year year]
             [(< year 1984)]]})
;; => #{["First Blood"] ["Alien"] ["Mad Max 2"] ["Mad Max"]}

;; another predicate
(q '{:find [name]
     :where [[p :person/name name]
             [(clojure.string/starts-with? name "M")]]})
;; => #{["Mark L. Lester"] ["Michael Preston"] ["Michael Biehn"] ["Marc de Jonge"] ["Mel Gibson"]}

;; exercise 1: find movies older than a certain year
(q '{:find [title]
     :in [year]
     :where [[m :movie/year movie-year]
             [m :movie/title title]
             [(<= movie-year year)]]}
   1984)
;; => #{["First Blood"] ["The Terminator"] ["Alien"] ["Mad Max 2"] ["Mad Max"]}

;; exercise 2: find actor older than dany glover
(q '{:find [actor]
     :where [[p1 :person/name "Danny Glover"]
             [p1 :person/born glover-birth]
             [p2 :person/born other-birth]
             [m :movie/cast p2]
             [(< other-birth glover-birth)] ;; compare birth of date
             [p2 :person/name actor]]})
;; => #{["Joe Pesci"] ["Brian Dennehy"] ["Tom Skerritt"] ["Alan Rickman"] ["Tina Turner"] ["Bruce Spence"] ["Michael Preston"] ["Charles Napier"] ["Gary Busey"] ["Sylvester Stallone"] ["Ronny Cox"] ["Richard Crenna"]}

(q '{:find [title]
     :in [year rating [[title r]]]
     :where [[m1 :movie/year y]
             [(>= y year)]
             [m1 :movie/title title]
             [(< rating r)]]}
   1990
   8.0
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
;; => #{["Terminator 2: Judgment Day"] ["Braveheart"]}

;;; transformation functions
(defn age [^java.util.Date birthday ^java.util.Date today]
  (quot (- (.getTime today)
           (.getTime birthday))
        (* 1000 60 60 24 365)))

;; find tina turner age
(q '{:find [age]
     :in [name today]
     :where [[p :person/name name]
             [p :person/born born]
             [(learn-xtdb/age born today) age]]}
   "Tina Turner"
   (java.util.Date.))
;; => #{[83]}

;; exercise 1: find people by age
(q '{:find [name]
     :in [age today]
     :where [[p :person/name name]
             [p :person/born born]
             [(learn-xtdb/age born today) age]
             ;;[(= a age)]
             ]}
   63
   #inst "2013-08-02T00:00:00.000-00:00")
;; => #{["Alexander Godunov"] ["Sigourney Weaver"] ["Nancy Allen"]}

;; exercise 2: find people younger than bruce willis
(q '{:find [name age]
     :in [today]
     :where [[b :person/name "Bruce Willis"]
             [b :person/born willis-born]
             [o :person/name name]
             [o :person/born other-born]
             [(learn-xtdb/age willis-born today) willis-age]
             [(learn-xtdb/age other-born today) age]
             [(< age willis-age)]]}
   (java.util.Date.))
;; => #{["Mel Gibson" 67] ["Nick Stahl" 43] ["Elpidia Carrillo" 61] ["Rae Dawn Chong" 62] ["Edward Furlong" 45] ["Alyssa Milano" 50] ["Linda Hamilton" 66] ["Claire Danes" 43] ["Sophie Marceau" 56] ["Michael Biehn" 66] ["Robert Patrick" 64] ["Jonathan Mostow" 61]}

;;; aggregates

;; exercise 1: count number of movies
(q '{:find [(count m)]
     :where [[m :movie/title]]})
;; => #{[20]}

;; exercise 2: find the oldest person
(q '{:find [(min date)]
     :where [[_ :person/born date]]})
;; => #{[#inst "1926-11-30T00:00:00.000-00:00"]}

;; exercise 3: find the avg rating
(q '{:find [name (avg rating)]
     :in [[name ...] [[title rating]]]
     :where [[p :person/name name]
             [m :movie/cast p]
             [m :movie/title title]]}
   ["Sylvester Stallone" "Arnold Schwarzenegger" "Mel Gibson"]
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
;; => #{["Arnold Schwarzenegger" 7.4799999999999995] ["Sylvester Stallone" 6.400000000000001] ["Mel Gibson" 7.200000000000001]}

;;; rules
;; abstractions, similar to function. reducing repetition.
;; example:
;;
;; from this:
;; [p :person/name name]
;; [m :movie/cast p]
;; [m :movie/title title]
;;
;; to:
;; [(actor-movie name title)
;;  [p :person/name name]
;;  [m :movie/cast p]
;;  [m :movie/title title]]

;; without rules
(q '{:find [name]
     :where [[p :person/name name]
             [m :movie/cast p]
             [m :movie/title "The Terminator"]]})
;; => #{["Michael Biehn"] ["Arnold Schwarzenegger"] ["Linda Hamilton"]}

;; with rules
(q '{:find [name]
     :where [(actor-movie name "The Terminator")]
     :rules [[(actor-movie name title)
              [p :person/name name]
              [m :movie/cast p]
              [m :movie/title title]]]})
;; => #{["Michael Biehn"] ["Arnold Schwarzenegger"] ["Linda Hamilton"]}

;; same rule name can be used several times to write logical OR
(q '{:find [name]
     :where [[m :movie/title "Predator"]
             (associated-with p m)
             [p :person/name name]]
     :rules [[(associated-with person movie)
              [movie :movie/cast person]]
             [(associated-with person movie)
              [movie :movie/director person]]]})
;; => #{["Carl Weathers"] ["John McTiernan"] ["Elpidia Carrillo"] ["Arnold Schwarzenegger"]}

;; exercise 1: write a rule (movie-year title year)
(q '{:find [title]
     :where [(movie-year title 1991)]
     :rules [[(movie-year title year)
              [m :movie/year year]
              [m :movie/title title]]]})
;; => #{["Terminator 2: Judgment Day"]}

;; exercise 2: write a rules which determine if one person is friend with another (in a mvie)
(q '{:find [friend]
     :in [name]
     :where [[p1 :person/name name]
             (friends p1 p2)
             [p2 :person/name friend]]
     :rules [[(friends ?p1 ?p2)
              [?m :movie/cast ?p1]
              [?m :movie/cast ?p2]
              [(not= ?p1 ?p2)]]
             [(friends ?p1 ?p2)
              [?m :movie/cast ?p1]
              [?m :movie/director ?p2]
              [(not= ?p1 ?p2)]]
             [(friends ?p1 ?p2)
              [?m :movie/director ?p1]
              [?m :movie/cast ?p2]
              [(not= ?p1 ?p2)]]]}
   "Sigourney Weaver")
;; => #{["Carrie Henn"] ["Tom Skerritt"] ["Ridley Scott"] ["Michael Biehn"] ["Veronica Cartwright"] ["James Cameron"]}
