(ns adqc.sql
  (:use [adqc.util :only [defextractors extract-info]]
        [clojure.zip :as zip :only []]
        [clojure.walk :as walk :only []]
        [clojure.contrib.string :as str :only []])
  (:import uk.org.ogsadai.parser.sql92query.SQLQueryParser
           org.antlr.runtime.tree.CommonTree
           org.antlr.runtime.CommonToken))

(let [sql-query-parser (SQLQueryParser/getInstance)]
  
  (defn antlr-parse-sql [sql]
    (.parseSQL sql-query-parser sql))
  
  (defn antlr-parse-condition [sql]
    (.parseSQLForCondition sql-query-parser sql))
  (defn antlr-parse-derived-column [sql]
    (.parseSQLForDerivedColumn sql-query-parser sql))
  (defn antlr-parse-literal [sql]
    (.parseSQLForLiteral sql-query-parser sql))
  (defn antlr-parse-value-expression-or-star [sql]
    (.parseSQLForValueExpressionOrStar sql-query-parser sql))
  
  (defn antlr-generate-sql [ast]
    (.generateSQL sql-query-parser ast))

  (defn antlr-generate-condition [ast]
    (.generateSQLForCondition sql-query-parser ast))
  (defn antlr-generate-query [ast]
    (.generateSQLForQuery sql-query-parser ast))
  (defn antlr-generate-value-expression-or-star [ast]
    (.generateSQLForValueExpressionOrStar sql-query-parser ast)))

(defn antlr-ast-zip
  [root]
  (zip/zipper (constantly true)
              (fn children [node]
                (let [cnt (.getChildCount ^CommonTree node)]
                  (map #(.getChild ^CommonTree node %) (range cnt))))
              ;; this is wrong, make-node should return
              ;; a *new* branch node
              #_
              (fn make-node [node ^java.util.Collection children]
                (.addChildren ^CommonTree node
                              (java.util.ArrayList. children)))
              (fn make-node [node children]
                (throw
                 (Exception.
                  "adding nodes to ANTLR AST zipper not supported")))
              root))

(defprotocol ANTLR->Clojure
  (antlr->clojure [self]))

(defextractors token-info-extractors CommonToken
  :ttype getType
  :text getText)

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
  :ntype getType
  :nkey (->> getText (str/replace-re #"_" "-") (str/lower-case) keyword)
  :token (-> getToken antlr->clojure)
  :text getText
  :children :node-children)

(extend-protocol ANTLR->Clojure
  CommonTree
  (antlr->clojure [self] (extract-info node-info-extractors self))
  CommonToken
  (antlr->clojure [self] (extract-info token-info-extractors self)))

#_
(defn parse-sql
  "Parses the given SQL with OGSA-DAI's ANTLR-based parser
  and converts the result to a Clojure tree."
  [sql]
  )
