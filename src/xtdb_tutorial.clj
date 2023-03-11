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


;;; part 4

;; use valid time
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :consumer/RJ29sUU
    :consumer-id :RJ29sUU
    :first-name "Jay"
    :last-name "Rose"
    :cover? true
    :cover-type :Full}
   #inst "2114-12-03"]])
;; => #:xtdb.api{:tx-id 7, :tx-time #inst "2023-03-09T08:51:01.026-00:00"}

(xt/sync node)
;; => #inst "2023-03-09T08:51:01.026-00:00"

;; bitemporality
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :consumer/RJ29sUU
    :consumer-id :RJ29sUU
    :first-name "Jay"
    :last-name "Rose"
    :cover? true
    :cover-type :Full}
   #inst "2113-12-03" ;; valid time start
   #inst "2114-12-03"] ;; valid time end

  [::xt/put
   {:xt/id :consumer/RJ29sUU
    :consumer-id :RJ29sUU
    :first-name "Jay"
    :last-name "Rose"
    :cover? true
    :cover-type :Full}
   #inst "2112-12-03"
   #inst "2113-12-03"]

  [::xt/put
   {:xt/id :consumer/RJ29sUU
    :consumer-id :RJ29sUU
    :first-name "Jay"
    :last-name "Rose"
    :cover? false}
   #inst "2112-06-03"
   #inst "2112-12-02"]

  [::xt/put
   {:xt/id :consumer/RJ29sUU
    :consumer-id :RJ29sUU
    :first-name "Jay"
    :last-name "Rose"
    :cover? true
    :cover-type :Promotional}
   #inst "2111-06-03"
   #inst "2112-06-03"]])
;; => #:xtdb.api{:tx-id 8, :tx-time #inst "2023-03-09T08:58:58.959-00:00"}

(xt/sync node)
;; => #inst "2023-03-09T08:58:58.959-00:00"

;; queries through time
(xt/q
 (xt/db node #inst "2114-01-01")
 '{:find [cover type]
   :where [[e :consumer-id :RJ29sUU]
           [e :cover? cover]
           [e :cover-type type]]})
;; => #{[true :Full]}

(xt/q
 (xt/db node #inst "2111-07-03")
 '{:find [cover type]
   :where [[e :consumer-id :RJ29sUU]
           [e :cover? cover]
           [e :cover-type type]]})
;; => #{[true :Promotional]}

(xt/q
 (xt/db node #inst "2112-07-03")
 '{:find [cover type]
   :where [[e :consumer-id :RJ29sUU]
           [e :cover? cover]
           [e :cover-type type]]})
;; => #{}

(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :manifest
    :pilot-name "Johanna"
    :id/rocket "SB002-sol"
    :id/employee "22910x2"
    :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP"]
    :cargo ["stereo" "gold fish" "slippers" "secret note"]}]])

(xt/sync node)
;; => #inst "2023-03-09T09:07:05.276-00:00"

;;; part 5
(def data-5
  [{:xt/id :gold-harmony
    :company-name "Gold Harmony"
    :seller? true
    :buyer? false
    :units/Au 10211
    :credits 51}

   {:xt/id :tombaugh-resources
    :company-name "Tombaugh Resources Ltd."
    :seller? true
    :buyer? false
    :units/Pu 50
    :units/N 3
    :units/CH4 92
    :credits 51}

   {:xt/id :encompass-trade
    :company-name "Encompass Trade"
    :seller? true
    :buyer? true
    :units/Au 10
    :units/Pu 5
    :units/CH4 211
    :credits 1002}

   {:xt/id :blue-energy
    :seller? false
    :buyer? true
    :company-name "Blue Energy"
    :credits 1000}])

(easy-ingest node data-5)
;; => #inst "2023-03-09T09:16:42.700-00:00"

;; check stock and fund level
(defn stock-check
  [company-id item]
  {:result (xt/q
            (xt/db node)
            {:find '[name funds stock]
             :where ['[e :company-name name]
                     '[e :credits funds]
                     ['e item 'stock]]
             :in '[e]}
            company-id)
   :item item})

(defn format-stock-check
  [{:keys [result item] :as stock-check}]
  (for [[name funds commod] result]
    (str "Name: " name ", Funds: " funds ", " item " " commod)))

;; move 10 units of methane for 100 credits to blue energy using match
(xt/submit-tx
 node
 [[::xt/match
   :blue-energy
   {:xt/id :blue-energy
    :seller? false
    :buyer? true
    :company-name "Blue Energy"
    :credits 1000}]

  [::xt/put
   {:xt/id :blue-energy
    :seller? false
    :buyer? true
    :company-name "Blue Energy"
    :credits 900
    :units/CH4 10}]

  [::xt/match
   :tombaugh-resources
   {:xt/id :tombaugh-resources
    :company-name "Tombaugh Resources Ltd."
    :seller? true
    :buyer? false
    :units/Pu 50
    :units/N 3
    :units/CH4 92
    :credits 51}]

  [::xt/put
   {:xt/id :tombaugh-resources
    :company-name "Tombaugh Resources Ltd."
    :seller? true
    :buyer? false
    :units/Pu 50
    :units/N 3
    :units/CH4 82
    :credits 151}]])
;; => #:xtdb.api{:tx-id 11, :tx-time #inst "2023-03-09T09:29:55.651-00:00"}

(xt/sync node)
;; => #inst "2023-03-09T09:29:55.651-00:00"

(format-stock-check (stock-check :tombaugh-resources :units/CH4))
;; => ("Name: Tombaugh Resources Ltd., Funds: 151, :units/CH4 82")

(format-stock-check (stock-check :blue-energy :units/CH4))
;; => ("Name: Blue Energy, Funds: 900, :units/CH4 10")

;; failed transaction because match is not valid
(xt/submit-tx
 node
 [[::xt/match
   :gold-harmony
   {:xt/id :gold-harmony
    :company-name "Gold Harmony"
    :seller? true
    :buyer? false
    :units/Au 10211
    :credits 51}]
  [::xt/put
   {:xt/id :gold-harmony
    :company-name "Gold Harmony"
    :seller? true
    :buyer? false
    :units/Au 211
    :credits 51}]

  [::xt/match
   :encompass-trade
   {:xt/id :encompass-trade
    :company-name "Encompass Trade"
    :seller? true
    :buyer? true
    :units/Au 10
    :units/Pu 5
    :units/CH4 211
    :credits 100002}]
  [::xt/put
   {:xt/id :encompass-trade
    :company-name "Encompass Trade"
    :seller? true
    :buyer? true
    :units/Au 10010
    :units/Pu 5
    :units/CH4 211
    :credits 1002}]])
;; => #:xtdb.api{:tx-id 12, :tx-time #inst "2023-03-09T09:34:52.367-00:00"}

(xt/sync node)
;; => #inst "2023-03-09T09:34:52.367-00:00"

(format-stock-check (stock-check :gold-harmony :units/Au))
;; => ("Name: Gold Harmony, Funds: 51, :units/Au 10211")

(format-stock-check (stock-check :encompass-trade :units/Au))
;; => ("Name: Encompass Trade, Funds: 1002, :units/Au 10")

(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :manifest
    :pilot-name "Johanna"
    :id/rocket "SB002-sol"
    :id/employee "22910x2"
    :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"]
    :cargo ["stereo" "gold fish" "slippers" "secret note"]}]])
;; => #:xtdb.api{:tx-id 13, :tx-time #inst "2023-03-09T09:36:41.099-00:00"}

(xt/sync node)
;; => #inst "2023-03-09T09:36:41.099-00:00"

(xt/q
 (xt/db node)
  '{:find [belongings]
    :where [[e :cargo belongings]]
    :in [belongings]}
  "secret note")
;; => #{["secret note"]}

;;; part 6

;; delete documents
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :kaarlang/clients
    :clients [:encompass-trade]}
   #inst "2110-01-01T09"
   #inst "2111-01-01T09"]

  [::xt/put
   {:xt/id :kaarlang/clients
    :clients [:encompass-trade :blue-energy]}
   #inst "2111-01-01T09"
   #inst "2113-01-01T09"]

  [::xt/put
   {:xt/id :kaarlang/clients
    :clients [:blue-energy]}
   #inst "2113-01-01T09"
   #inst "2114-01-01T09"]

  [::xt/put
   {:xt/id :kaarlang/clients
    :clients [:blue-energy :gold-harmony :tombaugh-resources]}
   #inst "2114-01-01T09"
   #inst "2115-01-01T09"]])
;; => #:xtdb.api{:tx-id 14, :tx-time #inst "2023-03-11T04:54:41.163-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T04:54:41.163-00:00"

;; view entity history
(xt/entity-history
 (xt/db node #inst "2116-01-01T09")
 :kaarlang/clients
 :desc
 {:with-docs? true})
;; => [#:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2115-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2114-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "d4bca6c78409d9d40ee42319a8aec32bffad9030", :doc {:clients [:blue-energy :gold-harmony :tombaugh-resources], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2113-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "000e5b775b55d06f0bddc77d736184284aa1e4e9", :doc {:clients [:blue-energy], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2111-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "cd71551fe21219db59067ce7483370fdebaae8b0", :doc {:clients [:encompass-trade :blue-energy], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2110-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "5ec42ea653288e01e1a9d7d2068b4658416177e0", :doc {:clients [:encompass-trade], :xt/id :kaarlang/clients}}]

;; delete documents
(xt/submit-tx
 node
 [[::xt/delete :kaarlang/clients #inst "2110-01-01" #inst "2116-01-01"]])
;; => #:xtdb.api{:tx-id 15, :tx-time #inst "2023-03-11T04:58:50.642-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T04:58:50.642-00:00"

;; view deletion results
(xt/entity-history
 (xt/db node #inst "2115-01-01T09")
 :kaarlang/clients
 :desc
 {:with-docs? true})
;; => [#:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2115-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2114-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2113-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2111-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2110-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2110-01-01T00:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil}]

;; retrieving deleted docs
(xt/entity-history
 (xt/db node #inst "2115-01-01T09")
 :kaarlang/clients
 :desc
 {:with-docs? true
  :with-corrections? true})
;; => [#:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2115-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2115-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2114-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2114-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "d4bca6c78409d9d40ee42319a8aec32bffad9030", :doc {:clients [:blue-energy :gold-harmony :tombaugh-resources], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2113-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2113-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "000e5b775b55d06f0bddc77d736184284aa1e4e9", :doc {:clients [:blue-energy], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2111-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2111-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "cd71551fe21219db59067ce7483370fdebaae8b0", :doc {:clients [:encompass-trade :blue-energy], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2110-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil} #:xtdb.api{:tx-time #inst "2023-03-11T04:54:41.163-00:00", :tx-id 14, :valid-time #inst "2110-01-01T09:00:00.000-00:00", :content-hash #xtdb/id "5ec42ea653288e01e1a9d7d2068b4658416177e0", :doc {:clients [:encompass-trade], :xt/id :kaarlang/clients}} #:xtdb.api{:tx-time #inst "2023-03-11T04:58:50.642-00:00", :tx-id 15, :valid-time #inst "2110-01-01T00:00:00.000-00:00", :content-hash #xtdb/id "0000000000000000000000000000000000000000", :doc nil}]

;;; part 7

;; data for eviction
(xt/submit-tx
 node
 [[::xt/put
   {:xt/id :person/kaarlang
    :full-name "Kaarlang"
    :origin-planet "Mars"
    :identity-tag :KA01299242093
    :DOB #inst "2040-11-23"}]

  [::xt/put
   {:xt/id :person/ilex
    :full-name "Ilex Jefferson"
    :origin-planet "Venus"
    :identity-tag :IJ01222212454
    :DOB #inst "2061-02-17"}]

  [::xt/put
   {:xt/id :person/thadd
    :full-name "Thad Christover"
    :origin-moon "Titan"
    :identity-tag :IJ01222212454
    :DOB #inst "2101-01-01"}]

  [::xt/put
   {:xt/id :person/johanna
    :full-name "Johanna"
    :origin-planet "Earth"
    :identity-tag :JA0129929120
    :DOB #inst "2090-12-07"}]])
;; => #:xtdb.api{:tx-id 16, :tx-time #inst "2023-03-11T05:12:13.796-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T05:12:13.796-00:00"

(defn full-query [node]
  (xt/q
   (xt/db node)
   '{:find [(pull e [*])]
     :where [[e :xt/id id]]}))

(full-query node)
;; => #{[{:common-name "Nitrogen", :type :element/gas, :density 1.2506, :radioactive false, :xt/id :commodity/N}] [{:seller? false, :buyer? true, :company-name "Blue Energy", :credits 900, :units/CH4 10, :xt/id :blue-energy}] [{:company-name "Gold Harmony", :seller? true, :buyer? false, :units/Au 10211, :credits 51, :xt/id :gold-harmony}] [{:pilot-name "Johanna", :id/rocket "SB002-sol", :id/employee "22910x2", :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"], :cargo ["stereo" "gold fish" "slippers" "secret note"], :xt/id :manifest}] [{:company-name "Tombaugh Resources Ltd.", :seller? true, :buyer? false, :units/Pu 50, :units/N 3, :units/CH4 82, :credits 151, :xt/id :tombaugh-resources}] [{:full-name "Kaarlang", :origin-planet "Mars", :identity-tag :KA01299242093, :DOB #inst "2040-11-23T00:00:00.000-00:00", :xt/id :person/kaarlang}] [{:common-name "Plutonium", :type :element/metal, :density 19.816, :radioactive true, :xt/id :commodity/Pu}] [{:common-name "Methane", :type :molecule/gas, :density 0.717, :radioactive false, :xt/id :commodity/CH4}] [{:full-name "Ilex Jefferson", :origin-planet "Venus", :identity-tag :IJ01222212454, :DOB #inst "2061-02-17T00:00:00.000-00:00", :xt/id :person/ilex}] [{:common-name "Carbon", :type :element/non-metal, :density 2.267, :radioactive false, :xt/id :commodity/C}] [{:common-name "Borax", :IUPAC-name "Sodium tetraborate decahydrate", :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"], :type :mineral/solid, :appearance "white solid", :density 1.73, :radioactive false, :xt/id :commodity/borax}] [{:full-name "Johanna", :origin-planet "Earth", :identity-tag :JA0129929120, :DOB #inst "2090-12-07T00:00:00.000-00:00", :xt/id :person/johanna}] [{:full-name "Thad Christover", :origin-moon "Titan", :identity-tag :IJ01222212454, :DOB #inst "2101-01-01T00:00:00.000-00:00", :xt/id :person/thadd}] [{:company-name "Encompass Trade", :seller? true, :buyer? true, :units/Au 10, :units/Pu 5, :units/CH4 211, :credits 1002, :xt/id :encompass-trade}] [{:common-name "Gold", :type :element/metal, :density 19.3, :radioactive false, :xt/id :commodity/Au}]}


;; evict kaarlang
(xt/submit-tx
 node
 [[::xt/evict :person/kaarlang]])
;; => #:xtdb.api{:tx-id 17, :tx-time #inst "2023-03-11T05:14:55.285-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T05:14:55.285-00:00"

(full-query node)
;; => #{[{:common-name "Nitrogen", :type :element/gas, :density 1.2506, :radioactive false, :xt/id :commodity/N}] [{:seller? false, :buyer? true, :company-name "Blue Energy", :credits 900, :units/CH4 10, :xt/id :blue-energy}] [{:company-name "Gold Harmony", :seller? true, :buyer? false, :units/Au 10211, :credits 51, :xt/id :gold-harmony}] [{:pilot-name "Johanna", :id/rocket "SB002-sol", :id/employee "22910x2", :badges ["SETUP" "PUT" "DATALOG-QUERIES" "BITEMP" "MATCH"], :cargo ["stereo" "gold fish" "slippers" "secret note"], :xt/id :manifest}] [{:company-name "Tombaugh Resources Ltd.", :seller? true, :buyer? false, :units/Pu 50, :units/N 3, :units/CH4 82, :credits 151, :xt/id :tombaugh-resources}] [{:common-name "Plutonium", :type :element/metal, :density 19.816, :radioactive true, :xt/id :commodity/Pu}] [{:common-name "Methane", :type :molecule/gas, :density 0.717, :radioactive false, :xt/id :commodity/CH4}] [{:full-name "Ilex Jefferson", :origin-planet "Venus", :identity-tag :IJ01222212454, :DOB #inst "2061-02-17T00:00:00.000-00:00", :xt/id :person/ilex}] [{:common-name "Carbon", :type :element/non-metal, :density 2.267, :radioactive false, :xt/id :commodity/C}] [{:common-name "Borax", :IUPAC-name "Sodium tetraborate decahydrate", :other-names ["Borax decahydrate" "sodium borate" "sodium tetraborate" "disodium tetraborate"], :type :mineral/solid, :appearance "white solid", :density 1.73, :radioactive false, :xt/id :commodity/borax}] [{:full-name "Johanna", :origin-planet "Earth", :identity-tag :JA0129929120, :DOB #inst "2090-12-07T00:00:00.000-00:00", :xt/id :person/johanna}] [{:full-name "Thad Christover", :origin-moon "Titan", :identity-tag :IJ01222212454, :DOB #inst "2101-01-01T00:00:00.000-00:00", :xt/id :person/thadd}] [{:company-name "Encompass Trade", :seller? true, :buyer? true, :units/Au 10, :units/Pu 5, :units/CH4 211, :credits 1002, :xt/id :encompass-trade}] [{:common-name "Gold", :type :element/metal, :density 19.3, :radioactive false, :xt/id :commodity/Au}]}

(xt/entity-history
 (xt/db node)
 :person/kaarlang
 :desc
 {:with-docs? true})
;; => []

;;; part 8

(def stats
  [{:body "Sun"
    :type "Star"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 109.3
    :volume 1305700
    :mass 33000
    :gravity 27.9
    :xt/id :Sun}
   {:body "Jupiter"
    :type "Gas Giant"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 10.97
    :volume 1321
    :mass 317.83
    :gravity 2.52
    :xt/id :Jupiter}
   {:body "Saturn"
    :type "Gas Giant"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius :volume
    :mass :gravity
    :xt/id :Saturn}
   {:body "Saturn"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 9.14
    :volume 764
    :mass 95.162
    :gravity 1.065
    :type "planet"
    :xt/id :Saturn}
   {:body "Uranus"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 3.981
    :volume 63.1
    :mass 14.536
    :gravity 0.886
    :type "planet"
    :xt/id :Uranus}
   {:body "Neptune"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 3.865
    :volume 57.7
    :mass 17.147
    :gravity 1.137
    :type "planet"
    :xt/id :Neptune}
   {:body "Earth"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 1
    :volume 1
    :mass 1
    :gravity 1
    :type "planet"
    :xt/id :Earth}
   {:body "Venus"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.9499
    :volume 0.857
    :mass 0.815
    :gravity 0.905
    :type "planet"
    :xt/id :Venus}
   {:body "Mars"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.532
    :volume 0.151
    :mass 0.107
    :gravity 0.379
    :type "planet"
    :xt/id :Mars}
   {:body "Ganymede"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.4135
    :volume 0.0704
    :mass 0.0248
    :gravity 0.146
    :type "moon"
    :xt/id :Ganymede}
   {:body "Titan"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.4037
    :volume 0.0658
    :mass 0.0225
    :gravity 0.138
    :type "moon"
    :xt/id :Titan}
   {:body "Mercury"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.3829
    :volume 0.0562
    :mass 0.0553
    :gravity 0.377
    :type "planet"
    :xt/id :Mercury}])

(xt/submit-tx
 node
 (mapv (fn [stat]
         [::xt/put stat])
       stats))
;; => #:xtdb.api{:tx-id 18, :tx-time #inst "2023-03-11T05:34:13.195-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T05:34:13.195-00:00"

(xt/submit-tx
 node
 [[::xt/put
   {:body "Kepra-5"
    :units {:radius "Earth Radius"
            :volume "Earth Volume"
            :mass "Earth Mass"
            :gravity "Standard gravity (g)"}
    :radius 0.6729
    :volume 0.4562
    :mass 0.5653
    :gravity 1.4
    :type "planet"
    :xt/id :Kepra-5}]])
;; => #:xtdb.api{:tx-id 19, :tx-time #inst "2023-03-11T05:36:43.917-00:00"}

(xt/sync node)
;; => #inst "2023-03-11T05:36:43.917-00:00"

(sort
 (xt/q
  (xt/db node)
  '{:find [g planet]
    :where [[planet :gravity g]]}))
;; => ([0.138 :Titan] [0.146 :Ganymede] [0.377 :Mercury] [0.379 :Mars] [0.886 :Uranus] [0.905 :Venus] [1 :Earth] [1.065 :Saturn] [1.137 :Neptune] [1.4 :Kepra-5] [2.52 :Jupiter] [27.9 :Sun])

(defn ingest-and-query
  [traveller-doc]
  (xt/submit-tx node [[::xt/put traveller-doc]])
  (xt/q
   (xt/db node)
   '{:find [n]
     :where [[id :passport-number n]]
     :in [id]}
   (:xt/id traveller-doc)))

;; async, so the first tries returned empty set
;; need to wait
(ingest-and-query
 {:xt/id :origin-planet/test-traveller
  :chosen-name "Test"
  :given-name "Test Traveller"
  :passport-number (java.util.UUID/randomUUID)
  :stamps []
  :penalties []})
;; => #{[#uuid "9f6f2eba-f995-4004-b69a-f2ff14a90051"]}
;; => #{}

;; use await
(defn ingest-and-query'
  "Ingest the given traveller's document, returns the passport number once the transaction is complete"
  [traveller-doc]
  (xt/await-tx
   node
   (xt/submit-tx node [[::xt/put traveller-doc]]))
  (xt/q
   (xt/db node)
   '{:find [n]
     :where [[id :passport-number n]]
     :in [id]}
   (:xt/id traveller-doc)))

(ingest-and-query'
 {:xt/id :origin-planet/new-test-traveller
  :chosen-name "Testy"
  :given-name "Testy Traveller"
  :passport-number (java.util.UUID/randomUUID)
  :stamps []
  :penalties []})
;; => #{[#uuid "1274a98b-1037-426f-abc4-766e334fcf98"]}

(ingest-and-query'
 {:xt/id :earth/ioelena
  :chosen-name "Ioelena"
  :given-name "Johanna"
  :passport-number (java.util.UUID/randomUUID)
  :stamps []
  :penalties []})
;; => #{[#uuid "854dd500-c600-4a70-acd0-13c068f2f417"]}
