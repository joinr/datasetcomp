(ns datasetcomp.patches
  (:require [spork.util.table]
            [tech.ml.dataset.csv]))

;;monkey patch to include tab-delimited csv file support.
(in-ns 'tech.ml.dataset.csv)
(defn csv->dataset
  [file-url & {:keys [nil-val separator quote]
               :or {nil-val -1
                    separator \,
                    quote \"}}]
  (with-open [in-stream (io/reader (io/input-stream file-url))]
    (let [csv-data (csv/read-csv in-stream :separator separator :quote quote)
          ln (first csv-data)
          map-keys (map keyword ln (first csv-data))
          csv-data (rest csv-data)
          ]
      [ln map-keys]
      (->> csv-data
           (mapv (fn [csv-line]
                   (when-not (= (count map-keys)
                                (count csv-line))
                     (throw (ex-info "Line contains bad data" {})))
                   (->> csv-line
                        (map #(try
                                (if (> (count %) 0)
                                  (Double/parseDouble %)
                                  nil-val)
                                (catch Throwable e
                                  (keyword %))))
                        (map vector map-keys)
                        (into {}))))))))

(in-ns 'spork.util.table)
;;We need to define operations to widen the schema dynamically...

;;added inference to the schema for lines->table
(defn lines->table 
  "Return a map-based table abstraction from reading lines of tabe delimited text.  
   The default string parser tries to parse an item as a number.  In 
   cases where there is an E in the string, parsing may return a number or 
   infinity.  Set the :parsemode key to any value to anything other than 
   :scientific to avoid parsing scientific numbers."
  [lines & {:keys [parsemode keywordize-fields? schema default-parser delimiter] 
            :or   {parsemode :scientific
                   keywordize-fields? true
                   schema {}
                   delimiter #"\t"}}] 
  (let [line->vec (s/->vector-splitter delimiter)
        tbl   (->column-table 
               (vec (map (if keywordize-fields?  
                           (comp keyword check-header clojure.string/trim)
                           identity)
                         (line->vec (general/first-any lines)))) 
               [])
        fields    (table-fields tbl)
        schema    (if (empty? schema)
                    (let [types (derive-schema (general/first-any (r/drop 1 lines))
                                               :parsemode parsemode
                                               :line->vec line->vec
                                               :fields fields)]
                      (into {} (map vector fields types)))
                    schema)
        parsef (pooled-parsing-scheme #_parse/parsing-scheme schema :default-parser  
                                      (or default-parser
                                          (if (= parsemode :scientific) parse/parse-string
                                              parse/parse-string-nonscientific)))

        parse-rec (comp (parse/vec-parser! fields parsef) line->vec) ;this makes garbage.
        ]
    (->> (conj-rows (empty-columns (count (table-fields tbl))) 
                    (r/map parse-rec (r/drop 1 lines)))
         (assoc tbl :columns))))

(in-ns 'datasetcomp.patches)
