(ns datasetcomp.core
  (:require #_[spork.util.table :as tbl]
            #_[tech.ml.dataset.csv :as tech]
            [tech.ml.dataset :as ds]
            #_[smile.io :as io]
            #_[datasetcomp.tablesaw :as tablesaw]
            [clj-memory-meter.core :as mm]))

(def the-data (atom nil))
;;atypical def usage, this is for cleaning up resources.
(defn clean! []
  (do (reset! the-data nil) (System/gc)))

(defmacro timed-build [name & expr]
  `(do  (println [:clearing-data-and-gcing])
         (clean!)
         (println [:evaluating ~name :as ~(list 'quote expr)])
         (reset! ~'datasetcomp.core/the-data (time ~@expr))
         (println [:measuring-memory-usage!])
         (println (mm/measure (deref ~'datasetcomp.core/the-data)))))
        
;;Monkey patched a couple of things into spork and tech
(comment
  (require 'datasetcomp.patches))
;;comparative testing...
(comment
  ;;about 197mb source file.
  ;;2386398 rows, 15 fields, mixture of numeric and text data.
  ;;available at 
  (def events "sampledata.txt")

  ;;To get a sense of file traversal, on my machine, just counting lines naively
  ;;via line-seq is almost 2x as slow as using reducers or other seq-avoiding
  ;;methods.  
  
  ;datasetcomp.core> (time (with-open [rdr (clojure.java.io/reader events)] (-> rdr line-seq count)))
  ;"Elapsed time: 666.917268 msecs"
  
  ;datasetcomp.core> (time (transduce (map (fn [_] 1)) (completing +) (spork.util.general/line-reducer events)))
  ;"Elapsed time: 394.14826 msecs"
  

  ;;This is doing naive schema inference, with no fallback or widening of the
  ;;type restrictions (it'll fail ungracefully currently, and the inference
  ;;is only based on the first row of data - a dumb hueristic
  (timed-build :spork-table (tbl/tabdelimited->table events))
  ;; [:clearing-data-and-gcing]
  ;; [:evalutating :blah :as ((tbl/tabdelimited->table events))]
  ;; "Elapsed time: 4173.821768 msecs"
  ;; [:measuring-memory-usage!]
  ;; 870.8 MB

  ;;I patched in the ability to pass through separators in tech,
  ;;other than that no modes.  I noticed that tech is holding onto
  ;;the head of a lazy sequence, which puts additional stress on
  ;;the GC (prior records can't be freed and are retained/realized
  ;;during processing).
  
  (timed-build :tech.csv    (tech/csv->dataset events :separator \tab))
  ;; [:clearing-data-and-gcing]
  ;; [:evalutating :tech.csv :as ((tech/csv->dataset events :separator 	))]
  ;; "Elapsed time: 75097.593756 msecs"
  ;; [:measuring-memory-usage!]
  ;; 1.5 GB

  ;;I normally use schemas for all my internal stuff, since I typically
  ;;know the types a-priori.  This is an example of deriving and
  ;;using a schema.
  
  ;;(spork.util.general/first-any (tbl/tabdelimited->records events))
  #_{:DemandGroup "Molly",
   :GhostFilled 0,
   :ACFilled 1,
   :SRC "01285K100",
   :TotalRequired 1,
   :DemandName "11436_Coco_01285K100_[1...760]",
   :TotalFilled 1,
   :NGFilled 0,
   :Quarter 1,
   :RCFilled 0,
   :t 1,
   :Vignette "Coco",
   :OtherFilled 0,
   :Overlapping 0,
   :Deployed 1}

  ;;based on the data from the first record, we can use the same heuristic
  ;;to build our table.  This time, if we pass in an explicit, typed
  ;;schema, we should use a different path to create the table.  Namely,
  ;;we end up using primitive columns where possible (i.e. for ints).
  ;;These are based on RRB-Vectors, since normal clojure primitive-backed
  ;;vectors didn't support transients at the time.  If we specify primitives
  ;;in the columns, then we should be able to leverage 
  
  (def schema 
    {:DemandGroup :text,
     :GhostFilled :int,
     :ACFilled :int,
     :SRC :text,
     :TotalRequired :int,
     :DemandName :text,
     :TotalFilled :int,
     :NGFilled :int,
     :Quarter :int,
     :RCFilled :int,
     :t :int,
     :Vignette :text,
     :OtherFilled :int,
     :Overlapping :int,
     :Deployed :int})

  ;;How much savings do we get..?
  (timed-build :spork.util.table-with-schema (tbl/tabdelimited->table events :schema schema))
  ;; [:clearing-data-and-gcing]
  ;; [:evalutating :spork.util.table-with-schema
  ;;   :as ((tbl/typed-lines->table (spork.util.general/line-reducer events) schema))]
  ;; "Elapsed time: 6332.616164 msecs"
  ;; [:measuring-memory-usage!]
  ;; 834.7 MB

  ;;Not much.


  ;;examining the potential cost savings...
  ;;rrb-vectors still have object arrays that contain trees and leaves, where the leaves
  ;;are primitives.

  (mm/measure (into (clojure.core.rrb-vector/vector-of :short) (repeat 1000000 10))) 
  "3.3 MB"
  (mm/measure (into (clojure.core.rrb-vector/vector-of :int) (repeat 1000000 10))) 
  "5.2 MB"
  
  ;;identical size, but we can't use transients for theoretically
  ;;faster construction (currently).
  (mm/measure (into (vector-of :int) (repeat 1000000 10))) 
  "5.2 MB"  
  (mm/measure (into (clojure.core.rrb-vector/vector-of :long) (repeat 1000000 10))) 
  "9.0 MB"
  (mm/measure (int-array (repeat 1000000 10)))
  "3.8 MB"
  (mm/measure (long-array (repeat 1000000 10)))
  "7.6 MB"
  (mm/measure (byte-array (repeat 1000000 10)))
  "976.6 KB"
  (defn only-fields [t type]
    (->> t
         (tbl/drop-fields (for [[k v] schema :when (= v type)]
                            k))))

  ;;datasetcomp.core> (mm/measure (only-fields @the-data :int))
  ;;"699.0 MB"

  ;;We can get some space savings if we shift to shorts apparently.
  ;;I don't have them in the spork.util.parsing/parse-defaults,
  ;;but they're easy enough to add ad-hoc.
  (defn ^short parse-short [^String v] (Short/parseShort v))
  
  (def short-schema
    (reduce-kv (fn [acc field parser]
                 (assoc acc field
                        (case parser
                          :int :short
                          parser)))
               {} schema))

  (spork.util.parsing/with-parsers
            {:short parse-short}
    (timed-build :short-fields
                 (tbl/tabdelimited->table events :schema short-schema)))
  ;;784.6 MB


  ;;a bit faster than spork, but check out the space savings...wow.
  ;;Keep in mind this is also mutable, although many operations in
  ;;the core API appear to do copy-on-write (from cursory scanning),
  ;;so it could be adapted for persistent cases.
  (timed-build :tablesaw
               (tablesaw/->table events :separator \tab))
  ;; [:clearing-data-and-gcing]
  ;; [:evaluating :tablesaw :as ((tablesaw/->table events :separator 	))]
  ;; "Elapsed time: 3987.573324 msecs"
  ;; [:measuring-memory-usage!]
  ;; 74.5 MB
  
  ;;got basic seq-of-maps abstraction, but nothing else at the moment.
  (first @the-data)
  ;;looks like it parsed correctly too.
  #_{"Vignette" "Katie", "OtherFilled" 0, "TotalRequired" 2, "ACFilled"
  0, "DemandName" "10661_Katie_16500FD00_[1...722]", "GhostFilled"
  0, "TotalFilled" 2, "t" 1, "Deployed"
  2, "SRC" "16500FD00", "DemandGroup" "Molly", "Quarter" 1, "NGFilled"
     1, "RCFilled" 1, "Overlapping" 0}

  ;;now using tech.ml.dataset 2.4
  (timed-build :tech.ml.dataset-2.0-beta-44    (ds/->dataset events))
  ;; [:clearing-data-and-gcing]
  ;; [:evaluating :tech.ml.dataset :as ((ds/->dataset events))]
  ;; "Elapsed time: 3593.6022 msecs"
  ;; [:measuring-memory-usage!]
  ;; 94.0 MB

  ;;comparing with SMILEs dataframe..
  (timed-build :smile.dataframe-2.4.0 (io/read-csv events))
  ;; [:clearing-data-and-gcing]
  ;; [:evaluating :smile.dataframe :as ((io/read-csv events))]
  ;; "Elapsed time: 2020.9463 msecs"
  ;; [:measuring-memory-usage!]
  ;; 482.8 MB


  ;;hmm, no change.
  (timed-build :tech.ml.dataset-2.0-beta-54    (ds/->dataset events))
  ;; [:clearing-data-and-gcing]
  ;; [:evaluating :tech.ml.dataset-2.0-beta-54 :as ((ds/->dataset events))]
  ;; "Elapsed time: 3655.1218 msecs"
  ;; [:measuring-memory-usage!]
  ;; 94.0 MB

  )

