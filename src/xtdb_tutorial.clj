(ns xtdb-tutorial
  (:require [xtdb.api :as xt]))

#_(defn halo []
  (println "halo"))

;;; part 1

;; create node 
(def node (xt/start-node {}))


;; create document: manifest
(def manifest
  {:xt/id :manifest
   :pilot-name "Johanna"
   :id/rocket "SB002-sol"
   :id/employee "22910x2"
   :badges "SETUP"
   :cargo ["stereo" "gold fish" "slippers" "secret note"]})

;; put the document to xtdb
(xt/submit-tx node [[::xt/put manifest]])
;; => #:xtdb.api{:tx-id 0, :tx-time #inst "2023-03-08T07:33:38.933-00:00"}

;; sync the node to get latest transaction
(xt/sync node)
;; => #inst "2023-03-08T07:33:38.933-00:00"

;; check the inserted document
(xt/entity (xt/db node) :manifest)
;; => {:pilot-name "Johanna", :id/rocket "SB002-sol", :id/employee "22910x2", :badges "SETUP", :cargo ["stereo" "gold fish" "slippers" "secret note"], :xt/id :manifest}

;;; part 2

;; put more documents: commodities
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :commodity/Pu
    :common-name "Plutonium"
    :type :element/metal
    :density 19.816
    :radioactive true}]
  [::xt/put
   {:xt/id :commodity/N
    :common-name "Nitrogen"
    :type :element/gas
    :density 1.2506
    :radioactive false}]
  [::xt/put
   {:xt/id :commodity/CH4
    :common-name "Methane"
    :type :molecule/gas
    :density 0.717
    :radioactive false}]])
;; => #:xtdb.api{:tx-id 1, :tx-time #inst "2023-03-08T07:48:33.561-00:00"}

(xt/sync node)
;; => #inst "2023-03-08T07:48:33.561-00:00"

;; put more data: stocks
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :stock/Pu
    :commod :commodity/Pu
    :weight-ton 21}
   #inst "2115-02-13T18"]

  [::xt/put
   {:xt/id :stock/Pu
    :commod :commodity/Pu
    :weight-ton 23}
   #inst "2115-02-14T18"]

  [::xt/put
   {:xt/id :stock/Pu
    :commod :commodity/Pu
    :weight-ton 22.2}
   #inst "2115-02-15T18"]

  [::xt/put
   {:xt/id :stock/Pu
    :commod :commodity/Pu
    :weight-ton 24}
   #inst "2115-02-18T18"]

  [::xt/put
   {:xt/id :stock/Pu
    :commod :commodity/Pu
    :weight-ton 24.9}
   #inst "2115-02-19T18"]])
;; => #:xtdb.api{:tx-id 2, :tx-time #inst "2023-03-08T07:55:44.163-00:00"}

(xt/sync node)
;; => #inst "2023-03-08T07:55:44.163-00:00"

(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :stock/N
    :commod :commodity/N
    :weight-ton 3}
   #inst "2115-02-13T18"
   #inst "2115-02-19T18"]

  [::xt/put
   {:xt/id :stock/CH4
    :commod :commodity/CH4
    :weight-ton 92}
   #inst "2115-02-15T18"
   #inst "2115-02-19T18"]])
;; => #:xtdb.api{:tx-id 3, :tx-time #inst "2023-03-08T07:58:32.224-00:00"}

;; query the documents
(xt/entity (xt/db node #inst "2115-02-14") :stock/Pu)
;; => {:commod :commodity/Pu, :weight-ton 21, :xt/id :stock/Pu}

(xt/entity (xt/db node #inst "2115-02-18") :stock/Pu)
;; => {:commod :commodity/Pu, :weight-ton 22.2, :xt/id :stock/Pu}

;; create function
(defn easy-ingest
  "uses xtdb put transaction to add a vector of documents to a specified node."
  [node docs]
  (xt/submit-tx node
                (vec (for [doc docs]
                       [::xt/put doc])))
  (xt/sync node))

;; update manifest
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :manifest
    :pilot-name "Johanna"
    :id/rocket "SB002-sol"
    :id/employee "22910x2"
    :badges ["SETUP" "PUT"]
    :cargo ["stereo" "gold fish" "slippers" "secret note"]}]])
;; => #:xtdb.api{:tx-id 4, :tx-time #inst "2023-03-08T08:04:11.237-00:00"}

;;; part 3
(def data
  [{:xt/id :commodity/Pu
    :common-name "Plutonium"
    :type :element/metal
    :density 19.816
    :radioactive true}

   {:xt/id :commodity/N
    :common-name "Nitrogen"
    :type :element/gas
    :density 1.2506
    :radioactive false}

   {:xt/id :commodity/CH4
    :common-name "Methane"
    :type :molecule/gas
    :density 0.717
    :radioactive false}

   {:xt/id :commodity/Au
    :common-name "Gold"
    :type :element/metal
    :density 19.300
    :radioactive false}

   {:xt/id :commodity/C
    :common-name "Carbon"
    :type :element/non-metal
    :density 2.267
    :radioactive false}

   {:xt/id :commodity/borax
    :common-name "Borax"
    :IUPAC-name "Sodium tetraborate decahydrate"
    :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"]
    :type :mineral/solid
    :appearance "white solid"
    :density 1.73
    :radioactive false}])

(easy-ingest node data)
;; => #inst "2023-03-08T08:13:47.225-00:00"

;; query

;; basic query
(xt/q (xt/db node)
      '{:find [element]
        :where [[element :type :element/metal]]})
;; => #{[:commodity/Pu] [:commodity/Au]}

;; quoting
(=
 (xt/q (xt/db node)
       '{:find [element]
         :where [[element :type :element/metal]]})

 (xt/q (xt/db node)
       {:find '[element]
        :where '[[element :type :element/metal]]})

 (xt/q (xt/db node)
       (quote
        {:find [element]
         :where [[element :type :element/metal]]})))
;; => true

;; return the name of metal elements
(xt/q (xt/db node)
      '{:find [name]
        :where [[e :type :element/metal]
                [e :common-name name]]})
;; => #{["Gold"] ["Plutonium"]}

;; more info
(xt/q (xt/db node)
      '{:find [name rho]
        :where [[e :density rho]
                [e :common-name name]]})
;; => #{["Nitrogen" 1.2506] ["Carbon" 2.267] ["Methane" 0.717] ["Borax" 1.73] ["Gold" 19.3] ["Plutonium" 19.816]}

;; arguments
(xt/q (xt/db node)
      '{:find [name]
        :where [[e :type type]
                [e :common-name name]]
        :in [type]}
      :element/metal)
;; => #{["Gold"] ["Plutonium"]}

(defn filter-type
  [type]
  (xt/q (xt/db node)
        '{:find [name]
          :where [[e :common-name name]
                  [e :type type]]
          :in [type]}
        type))

(defn filter-appearance
  [description]
  (xt/q (xt/db node)
        '{:find [name IUPAC]
          :where [[e :common-name name]
                  [e :IUPAC-name IUPAC]
                  [e :appearance appearance]]
          :in [appearance]}
        description))

(filter-type :element/metal)
;; => #{["Gold"] ["Plutonium"]}

(filter-appearance "white solid")
;; => #{["Borax" "Sodium tetraborate decahydrate"]}

;; update manifest
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :manifest
    :pilot-name "Johanna"
    :id/rocket "SB002-sol"
    :id/employee "22910x2"
    :badges ["SETUP" "PUT" "DATALOG-QUERIES"]
    :cargo ["stereo" "gold fish" "slippers" "secret note"]}]])

(xt/sync node)
;; => #inst "2023-03-08T08:33:37.331-00:00"
