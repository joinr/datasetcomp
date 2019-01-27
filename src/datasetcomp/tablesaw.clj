;;Minimal Wrapper Around tablesaw.
(ns datasetcomp.tablesaw
  (:import [tech.tablesaw.api
            Table
            Row]
           [tech.tablesaw.io.csv
            CsvReadOptions]))

;; CsvReadOptionsBuilder builder = 
;; 	CsvReadOptions.builder("myFile.csv")
;; 		.separator('\t')			// table is tab-delimited
;; 		.header(false)				// no header
;; 		.dateFormat("yyyy.MM.dd");  // the date format to use. 


(defn ^tech.tablesaw.io.csv.CsvReadOptions$Builder
  ->csv-builder [path & {:keys [separator header? date-format]}]
  (doto (CsvReadOptions/builder path)
    (.separator separator)
    (.header (boolean header?))
    ))
  
(defn  tsv-opts [path] (->csv-builder path))

(defn row-map [^Row r]
  (let [cs (.columnNames r)]
    (zipmap
     cs
     (map (fn [^String nm]
            (.getObject r nm)) cs))))

(deftype tablesaw [^Table host]
  clojure.lang.ISeq 
  (seq [this] (->>  (.iterator host)
                    iterator-seq
                    (map row-map)))
  (first [this] (first (seq this)))
  (next [this]  (next (seq this)))
  ;;necessary?
  (more [this]  (next (seq this)))
  clojure.lang.Indexed
  (nth [this n]
    (doto (Row. host) (.at n )))
  (nth [this n not-found])
  clojure.lang.Counted
  (count [this] (.rowCount host)))

;; Table t1 = Table.read().csv(options);
(defn ->table [path & {:keys [separator quote]}]
  (-> (Table/read)
      (.csv (->csv-builder path :separator separator :header? true))
      (->tablesaw)))



;;basic row-map abstraction...
;; (extend-type tech.tablesaw.api.Row
;;   clojure.lang.ISeq
;;   (seq [this]

;;      )
