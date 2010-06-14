(ns adqc.sql.protocols
  (:use [adqc.util :only [defextractors extract-info]]
        [clojure [string :as str :only []]])
  (:import #_uk.org.ogsadai.parser.sql92query.SQLQueryParser
           org.antlr.runtime.tree.CommonTree
           org.antlr.runtime.CommonToken))

(defprotocol ANTLR->Clojure
  (antlr->clojure [self]))

(defextractors token-info-extractors CommonToken
  :ttype :type
  :text  :text)

(defn node-children [^CommonTree node]
  (let [cnt (.getChildCount node)]
    (loop [i 0 children []]
      (if (< i cnt)
        (recur (inc i)
               (conj children
                     (-> (.getChild node i)
                         antlr->clojure)))
        children))))

(defextractors node-info-extractors CommonTree
  :ntype    :type
  :nkey     (->> :text (str/replace #"_" "-") (str/lower-case) keyword)
  :token    (-> :token antlr->clojure)
  :text     :text
  :children node-children)

(extend-protocol ANTLR->Clojure
  CommonTree
  (antlr->clojure [self] (extract-info node-info-extractors self))
  CommonToken
  (antlr->clojure [self] (extract-info token-info-extractors self)))

(defprotocol ToSQL
  (to-sql [self]))

(extend-protocol ToSQL
  String
  ;; no escaping done by me -- I'm dealing with SQL queries which should
  ;; have well-prepared strings!
  (to-sql [self] self)
  Number
  (to-sql [self] (str self)))

(defprotocol SQLExpression
  (attributes [self])
  (rename-attributes [self m]))

(extend-protocol SQLExpression
  String
  (attributes [_] #{})
  (rename-attributes [self _] self)
  Number
  (attributes [_] #{})
  (rename-attributes [self _] self))
